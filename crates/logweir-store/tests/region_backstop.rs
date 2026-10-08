//! FX-20 fix round, review F1: the store's own backstop. On an endpoint-less
//! S3 location `object_store` builds the host from the region
//! (`s3.<region>.amazonaws.com`), so no constructor builds a client over a
//! region that is not a region name — whether the location names it or, on
//! the legacy `from_env` path, the process environment supplies it. Every
//! runner refuses the same location earlier, by name; this is the rule held
//! where the client is built.
//!
//! ONE test in this binary on purpose: it sets `AWS_REGION` and
//! `AWS_DEFAULT_REGION`, which no other test may observe half-way.

use logweir_core::engine::StorageUrl;
use logweir_store::{CredentialSource, Store, StoreOptions};

fn s3(region: Option<&str>, prefix: &str) -> StorageUrl {
    StorageUrl::S3 {
        bucket: "victim-backups".into(),
        prefix: prefix.into(),
        region: region.map(str::to_string),
        endpoint: None,
        path_style: false,
        allow_http: false,
    }
}

fn options() -> StoreOptions {
    StoreOptions {
        credentials: CredentialSource::Static {
            access_key_id: ["AKIA", "FX20", "BACKSTOP"].concat(),
            secret_access_key: "fx20-backstop-not-a-secret".to_string(),
            session_token: None,
        },
        ..StoreOptions::default()
    }
}

fn refused(result: Result<Store, logweir_store::StoreError>, label: &str) {
    let error = result
        .err()
        .unwrap_or_else(|| panic!("{label}: a client was built"));
    let text = error.to_string();
    assert!(text.contains("StorageRegionInvalid"), "{label}: {text}");
    assert!(
        !text.contains("attacker"),
        "{label}: never the value: {text}"
    );
}

#[test]
fn fx20_no_constructor_builds_a_client_over_a_region_that_is_not_a_region_name() {
    std::env::remove_var("AWS_REGION");
    std::env::remove_var("AWS_DEFAULT_REGION");
    for region in [
        "x@attacker.example/",
        "us-east-1.attacker.example#",
        "US-EAST-1",
    ] {
        refused(Store::from_url(&s3(Some(region), "logweir/")), "from_url");
        refused(
            Store::read_only_from_url(&s3(Some(region), "team-a")),
            "read_only_from_url",
        );
        refused(
            Store::from_url_with(&s3(Some(region), "logweir/"), &options()),
            "from_url_with",
        );
    }
    // CONTROL: real regions, and no region, build (building dials nothing).
    for region in [Some("us-east-1"), Some("eu-west-2"), None] {
        assert!(
            Store::from_url(&s3(region, "logweir/")).is_ok(),
            "{region:?}"
        );
        assert!(
            Store::read_only_from_url(&s3(region, "team-a")).is_ok(),
            "{region:?}"
        );
        assert!(
            Store::from_url_with(&s3(region, "logweir/"), &options()).is_ok(),
            "{region:?}"
        );
    }

    // The legacy path's environment fall-back is held to the same rule — and
    // only when the location names no region of its own.
    for var in ["AWS_REGION", "AWS_DEFAULT_REGION"] {
        std::env::set_var(var, "x@attacker.example/");
        refused(Store::read_only_from_url(&s3(None, "team-a")), var);
        refused(Store::from_url(&s3(None, "logweir/")), var);
        assert!(
            Store::read_only_from_url(&s3(Some("us-east-1"), "team-a")).is_ok(),
            "{var}: a location naming its region does not read the environment's"
        );
        std::env::set_var(var, "eu-west-1");
        assert!(
            Store::read_only_from_url(&s3(None, "team-a")).is_ok(),
            "{var}"
        );
        std::env::remove_var(var);
    }
}
