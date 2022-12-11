use std::{env, sync::Arc};

use futures_util::{self, TryStreamExt};
use http::StatusCode;
use warp::{
    hyper::{Body, Response},
    Filter,
};

use crate::{
    cache::types::StreamError,
    npm::{self, NpmCache},
};

pub fn with_cache<'a>(
    cache: Arc<NpmCache>,
) -> impl Filter<Extract = (Arc<NpmCache>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || cache.clone())
}

pub async fn start_server() {
    let cache = npm::NpmCache::new(env::current_dir().unwrap());
    let incoming_log = warp::log::custom(|info| {
        eprintln!("GET http://127.0.0.1:4873{}", info.path(),);
    });
    let test = warp::path("test").map(|| "test");
    let fallback = warp::any().and(warp::path!(String)).and_then(unhandled);
    let routes = test
        .or(warp::path!(String / String / "-" / String)
            .and(with_cache(cache.clone()))
            .and_then(get_scoped_package_tarball))
        .or(warp::path!(String / "-" / String)
            .and(with_cache(cache.clone()))
            .and_then(get_package_tarball))
        .or(warp::path!(String / String)
            .and(with_cache(cache.clone()))
            .and_then(get_scoped_package_metadata))
        .or(warp::path!(String)
            .and(with_cache(cache.clone()))
            .and_then(get_package_metadata))
        .or(fallback)
        .with(incoming_log);
    warp::serve(routes).run(([127, 0, 0, 1], 4873)).await;
}

async fn unhandled(_path: String) -> Result<impl warp::Reply, warp::Rejection> {
    Ok(StatusCode::NOT_FOUND)
}

async fn get_package_metadata(
    package_name: String,
    cache: Arc<NpmCache>,
) -> Result<impl warp::Reply, warp::Rejection> {
    get_metadata_helper(
        npm::MetadataKey {
            package_scope: None,
            package_name,
        },
        cache,
    )
    .await
}

async fn get_scoped_package_metadata(
    at_scope: String,
    package_name: String,
    cache: Arc<NpmCache>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let scope = &at_scope[1..at_scope.len()];
    get_metadata_helper(
        npm::MetadataKey {
            package_scope: Some(scope.to_string()),
            package_name,
        },
        cache,
    )
    .await
}

async fn get_package_tarball(
    package_name: String,
    file_name: String,
    cache: Arc<NpmCache>,
) -> Result<impl warp::Reply + Send + Sync, warp::Rejection> {
    get_package_tarball_helper(
        npm::PackageKey {
            package_scope: None,
            package_name,
            tgz_filename: file_name,
        },
        cache,
    )
    .await
}

async fn get_scoped_package_tarball(
    at_scope: String,
    package_name: String,
    file_name: String,
    cache: Arc<NpmCache>,
) -> Result<impl warp::Reply + Send + Sync, warp::Rejection> {
    let scope = &at_scope[1..at_scope.len()];
    get_package_tarball_helper(
        npm::PackageKey {
            package_scope: Some(scope.to_string()),
            package_name,
            tgz_filename: file_name,
        },
        cache,
    )
    .await
}

async fn get_package_tarball_helper(
    key: npm::PackageKey,
    cache: Arc<NpmCache>,
) -> Result<impl warp::Reply + Send + Sync, warp::Rejection> {
    let maybe_stream = cache.get_package(&key).await;
    let entry = match maybe_stream {
        Ok(value) => value,
        Err(err) => {
            match err {
                StreamError::UnknownNetworError(err) => {
                    println!("error {:?} {:?}", err.clone().status(), err.to_string());
                }
                _ => {
                    println!("error");
                }
            }
            return Err(warp::reject());
        }
    };
    let body = Body::wrap_stream(entry.stream.map_err(|err| {
        println!("stream_error");
        err
    }));
    Ok(Response::new(body))
}

async fn get_metadata_helper(
    key: npm::MetadataKey,
    cache: Arc<NpmCache>,
) -> Result<impl warp::Reply + Send + Sync, warp::Rejection> {
    let maybe_stream = cache.get_metadata(&key).await;
    let entry = match maybe_stream {
        Ok(value) => value,
        Err(_err) => {
            println!("error");
            return Err(warp::reject());
        }
    };
    let body = Body::wrap_stream(entry.stream.map_err(|err| {
        println!("stream_error");
        err
    }));
    Ok(Response::new(body))
}
