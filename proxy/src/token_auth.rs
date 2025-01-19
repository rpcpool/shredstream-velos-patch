use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime},
};

use arc_swap::{ArcSwap, ArcSwapAny};
use jito_protos::{
    auth::{
        auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
        GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role, Token,
    },
    shredstream::shredstream_client::ShredstreamClient,
};
use prost_types::Timestamp;
use solana_sdk::signature::{Keypair, Signer};
use tokio::{runtime::Runtime, task::JoinHandle, time::sleep};
use tonic::{
    metadata::errors::InvalidMetadataValue,
    service::{interceptor::InterceptedService, Interceptor},
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request, Status,
};

use crate::error::ShredstreamError;

pub struct TokenAuthenticator {
    client: AuthServiceClient<Channel>,
    keypair: Arc<Keypair>,
    exit: Arc<AtomicBool>,
    runtime: Arc<Runtime>,
    block_engine_url: String,
}

impl TokenAuthenticator {
    pub fn new(
        block_engine_url: String,
        auth_url: Option<String>,
        keypair: Arc<Keypair>,
        runtime: Arc<Runtime>,
    ) -> Result<Self, ShredstreamError> {
        let auth_url = auth_url.unwrap_or_else(|| block_engine_url.clone());

        // Enter runtime context
        let _guard = runtime.enter();

        let channel = runtime.block_on(async {
            create_grpc_channel(auth_url)
                .await
                .map_err(ShredstreamError::Transport)
        })?;

        Ok(Self {
            client: AuthServiceClient::new(channel),
            keypair,
            exit: Arc::new(AtomicBool::new(false)),
            runtime,
            block_engine_url,
        })
    }

    pub fn create_shredstream_client(
        &self,
    ) -> Result<ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>, ShredstreamError>
    {
        // Enter runtime context
        let _guard = self.runtime.enter();

        let (interceptor, refresh_handle) = self
            .runtime
            .block_on(async {
                ClientInterceptor::new(
                    self.client.clone(),
                    self.keypair.clone(),
                    Role::ShredstreamSubscriber,
                    "shredstream".to_string(),
                    self.exit.clone(),
                )
                .await
            })
            .map_err(|e| ShredstreamError::Auth(e.to_string()))?;

        tokio::spawn(async move {
            refresh_handle.await.unwrap();
        });

        let channel = {
            let _guard = self.runtime.enter();
            self.runtime.block_on(async {
                create_grpc_channel(self.block_engine_url.clone())
                    .await
                    .map_err(ShredstreamError::Transport)
            })?
        };

        Ok(ShredstreamClient::with_interceptor(channel, interceptor))
    }

    pub fn client(&self) -> AuthServiceClient<Channel> {
        self.client.clone()
    }
}

async fn create_grpc_channel(url: String) -> Result<Channel, tonic::transport::Error> {
    let endpoint = match url.starts_with("https") {
        true => Endpoint::from_shared(url)?.tls_config(ClientTlsConfig::new())?,
        false => Endpoint::from_shared(url)?,
    };
    endpoint.connect().await
}

#[derive(Clone)]
pub struct ClientInterceptor {
    bearer_token: Arc<ArcSwap<String>>,
}

impl ClientInterceptor {
    async fn new(
        mut auth_service_client: AuthServiceClient<Channel>,
        keypair: Arc<Keypair>,
        role: Role,
        service_name: String,
        exit: Arc<AtomicBool>,
    ) -> Result<(Self, JoinHandle<()>), Status> {
        let (
            Token {
                value: access_token,
                expires_at_utc: access_token_expiration,
            },
            refresh_token,
        ) = Self::auth(&mut auth_service_client, &keypair, role).await?;

        let bearer_token = Arc::new(ArcSwap::from_pointee(access_token));
        let refresh_thread_handle = Self::spawn_token_refresh_thread(
            auth_service_client,
            bearer_token.clone(),
            refresh_token,
            access_token_expiration.unwrap(),
            keypair,
            role,
            service_name,
            exit,
        );

        Ok((Self { bearer_token }, refresh_thread_handle))
    }

    async fn auth(
        auth_service_client: &mut AuthServiceClient<Channel>,
        keypair: &Keypair,
        role: Role,
    ) -> Result<(Token, Token), Status> {
        let pubkey_vec = keypair.pubkey().as_ref().to_vec();
        let challenge_resp = auth_service_client
            .generate_auth_challenge(GenerateAuthChallengeRequest {
                role: role as i32,
                pubkey: pubkey_vec.clone(),
            })
            .await?
            .into_inner();

        let challenge = format!("{}-{}", keypair.pubkey(), challenge_resp.challenge);
        let signed_challenge = keypair.sign_message(challenge.as_bytes()).as_ref().to_vec();

        let tokens = auth_service_client
            .generate_auth_tokens(GenerateAuthTokensRequest {
                challenge,
                client_pubkey: pubkey_vec,
                signed_challenge,
            })
            .await?
            .into_inner();

        Ok((
            tokens
                .access_token
                .ok_or_else(|| Status::internal("Missing access token"))?,
            tokens
                .refresh_token
                .ok_or_else(|| Status::internal("Missing refresh token"))?,
        ))
    }

    fn spawn_token_refresh_thread(
        mut auth_service_client: AuthServiceClient<Channel>,
        bearer_token: Arc<ArcSwap<String>>,
        initial_refresh_token: Token,
        initial_access_token_expiration: Timestamp,
        keypair: Arc<Keypair>,
        role: Role,
        _service_name: String,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut refresh_token = initial_refresh_token;
            let mut access_token_expiration = initial_access_token_expiration;

            while !exit.load(Ordering::Relaxed) {
                let now = SystemTime::now();
                let refresh_token_ttl =
                    SystemTime::try_from(refresh_token.expires_at_utc.as_ref().unwrap().clone())
                        .unwrap()
                        .duration_since(now)
                        .unwrap_or_default();
                // Re-run entire auth workflow if refresh token expiring soon
                if refresh_token_ttl < Duration::from_secs(5 * 60) {
                    let start = Instant::now();
                    if let Ok((new_access_token, new_refresh_token)) =
                        Self::auth(&mut auth_service_client, &keypair, role).await
                    {
                        bearer_token.store(Arc::new(new_access_token.value));
                        access_token_expiration = new_access_token.expires_at_utc.unwrap();
                        refresh_token = new_refresh_token;
                        tracing::info!(
                            "Full auth refresh completed in {}ms",
                            start.elapsed().as_millis()
                        );
                    }
                    continue;
                }

                let access_token_ttl = SystemTime::try_from(access_token_expiration.clone())
                    .unwrap()
                    .duration_since(now)
                    .unwrap_or_default();

                if access_token_ttl < Duration::from_secs(5 * 60) {
                    let start = Instant::now();
                    if let Ok(refresh_resp) = auth_service_client
                        .refresh_access_token(RefreshAccessTokenRequest {
                            refresh_token: refresh_token.value.clone(),
                        })
                        .await
                    {
                        let access_token = refresh_resp.into_inner().access_token.unwrap();
                        bearer_token.store(Arc::new(access_token.value.clone()));
                        access_token_expiration = access_token.expires_at_utc.unwrap();
                        tracing::info!(
                            "Token refresh completed in {}ms",
                            start.elapsed().as_millis()
                        );
                    }
                    continue;
                }

                sleep(Duration::from_secs(5)).await;
            }
        })
    }
}

impl Interceptor for ClientInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let token = ArcSwapAny::load(&self.bearer_token);
        if token.is_empty() {
            return Err(Status::invalid_argument("missing bearer token"));
        }

        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token)
                .parse()
                .map_err(|e: InvalidMetadataValue| Status::invalid_argument(e.to_string()))?,
        );

        Ok(request)
    }
}
