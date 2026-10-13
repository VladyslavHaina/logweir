//! **PROD-05.1a: the controller reads a Strimzi cluster's `KafkaTopic`
//! resources** for a `Backup` whose `KafkaCluster` names its Strimzi cluster
//! (`spec.strimzi {namespace, cluster}`).
//!
//! It lists `kafkatopics.kafka.strimzi.io` in that namespace with the label
//! `strimzi.io/cluster=<cluster>`, keeps the resources that name one of the
//! run's topics and are not unmanaged (`strimzi.io/managed: "false"`), and
//! freezes them into the run's `execution-inputs.json`
//! ([`crate::backup_execution::KafkaTopicResourcesInputs`]); the runner reads
//! them as it reads `--kafka-topic-resources`, and the receipt records
//! `owner_detection: [kafkaTopicResources]`.
//!
//! **Read only, opt-in, and only where the chart says.** The chart grants
//! `get` and `list` on `kafkatopics` in `kafka.strimzi.io`, through a Role and
//! RoleBinding in each namespace `strimzi.namespaces` lists, only with
//! `strimzi.readKafkaTopics: true` (default `false`), and renders the same
//! list into [`STRIMZI_NAMESPACES_ENV`]. A `KafkaCluster` naming any other
//! namespace is refused before any API call ([`admit_namespace`]): whoever can
//! write a `KafkaCluster` cannot point the controller's read, and the receipt
//! it feeds, at a namespace the installation did not open to it. This module
//! never creates, changes or deletes a `KafkaTopic` (ADR 0008 Amendment K
//! (4)).
//!
//! **Fails closed.** When `spec.strimzi` is set and the list is refused (403)
//! or the resource is not served (404 for every version), the run is refused
//! `OwnerDetectionFailed`: a receipt that said "looked, none found" would send
//! a restore around an owner nobody checked for. Any other API failure is a
//! requeue.

use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, ListParams};

use crate::backup_execution::{KafkaTopicResourceInputs, KafkaTopicResourcesInputs};
use crate::crds::kafka_cluster::{KafkaCluster, StrimziRef};

/// The API group.
pub const STRIMZI_GROUP: &str = "kafka.strimzi.io";

/// The versions listed, in order: the first one the API server serves is
/// read.
pub const KAFKA_TOPIC_VERSIONS: [&str; 2] = ["v1beta2", "v1"];

/// The most `KafkaTopic` resources one list reads, in pages of
/// [`PAGE_SIZE`]. More fails closed: an owner the read did not reach would be
/// recorded as no owner.
pub const MAX_KAFKA_TOPICS: usize = 10_000;

/// One page.
pub const PAGE_SIZE: u32 = 500;

/// The controller's environment variable naming the namespaces it may read
/// `KafkaTopic`s in, comma-separated: the chart's `strimzi.namespaces`, set
/// only with `strimzi.readKafkaTopics: true`.
pub const STRIMZI_NAMESPACES_ENV: &str = "LOGWEIR_STRIMZI_NAMESPACES";

/// The namespaces [`STRIMZI_NAMESPACES_ENV`] lists; empty when it is unset.
#[must_use]
pub fn configured_namespaces(raw: Option<&str>) -> Vec<String> {
    raw.unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .map(str::to_string)
        .collect()
}

/// This process's [`configured_namespaces`].
#[must_use]
pub fn process_namespaces() -> Vec<String> {
    configured_namespaces(std::env::var(STRIMZI_NAMESPACES_ENV).ok().as_deref())
}

/// Why a `KafkaCluster`'s Strimzi namespace may not be read, or `None`: it
/// must be one the installation listed.
#[must_use]
pub fn admit_namespace(strimzi: &StrimziRef, allowed: &[String]) -> Option<String> {
    if allowed.contains(&strimzi.namespace) {
        return None;
    }
    Some(format!(
        "the KafkaCluster's spec.strimzi.namespace `{}` is not a namespace this installation \
         lets the controller read KafkaTopics in (the chart's strimzi.namespaces, with \
         strimzi.readKafkaTopics: true, lists {}); no KafkaTopic was read",
        strimzi.namespace,
        if allowed.is_empty() {
            "none".to_string()
        } else {
            allowed.join(", ")
        }
    ))
}

/// Why the list did not produce an answer.
#[derive(Debug)]
pub enum StrimziListError {
    /// Refused or not served: `OwnerDetectionFailed`, with the sentence.
    Refused(String),
    /// Anything else the API server answered: transient, a requeue.
    Api(kube::Error),
}

/// **The read.** `Ok(None)` when the `KafkaCluster` names no Strimzi
/// cluster: nothing is listed. Otherwise every listed `KafkaTopic` that
/// names one of `topics`, possibly none, from a namespace in `allowed` only.
///
/// # Errors
///
/// [`StrimziListError::Refused`] for a namespace not in `allowed` (before
/// any API call), a 403, a resource no version of which is served, or more
/// than [`MAX_KAFKA_TOPICS`]; [`StrimziListError::Api`] for any other failure.
pub async fn resolve(
    client: &kube::Client,
    cluster: &KafkaCluster,
    topics: &[String],
    allowed: &[String],
) -> Result<Option<KafkaTopicResourcesInputs>, StrimziListError> {
    let Some(strimzi) = cluster.spec.strimzi.as_ref() else {
        return Ok(None);
    };
    if let Some(why) = admit_namespace(strimzi, allowed) {
        return Err(StrimziListError::Refused(why));
    }
    let items = list(client, strimzi).await?;
    Ok(Some(select(strimzi, &items, topics)))
}

async fn list(
    client: &kube::Client,
    strimzi: &StrimziRef,
) -> Result<Vec<DynamicObject>, StrimziListError> {
    let selector = format!("strimzi.io/cluster={}", strimzi.cluster);
    let mut not_served = Vec::new();
    for version in KAFKA_TOPIC_VERSIONS {
        let resource = ApiResource::from_gvk_with_plural(
            &GroupVersionKind::gvk(STRIMZI_GROUP, version, "KafkaTopic"),
            "kafkatopics",
        );
        let api: Api<DynamicObject> =
            Api::namespaced_with(client.clone(), &strimzi.namespace, &resource);
        let mut items = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut params = ListParams::default().labels(&selector).limit(PAGE_SIZE);
            if let Some(t) = token.take() {
                params = params.continue_token(&t);
            }
            match api.list(&params).await {
                Ok(page) => {
                    items.extend(page.items);
                    if items.len() > MAX_KAFKA_TOPICS {
                        return Err(StrimziListError::Refused(format!(
                            "namespace {} holds more than {MAX_KAFKA_TOPICS} KafkaTopic \
                             resources labelled strimzi.io/cluster={}; an owner the read did \
                             not reach would be recorded as no owner",
                            strimzi.namespace, strimzi.cluster
                        )));
                    }
                    match page.metadata.continue_.filter(|c| !c.is_empty()) {
                        Some(next) => token = Some(next),
                        None => return Ok(items),
                    }
                }
                Err(kube::Error::Api(e)) if e.code == 403 => {
                    return Err(StrimziListError::Refused(format!(
                        "the controller may not list kafkatopics.{STRIMZI_GROUP} in namespace {} \
                         (403); set the chart values strimzi.readKafkaTopics: true and \
                         strimzi.namespaces to include it, which grant get and list on them there \
                         and nothing else, or remove spec.strimzi from the KafkaCluster",
                        strimzi.namespace
                    )));
                }
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    not_served.push(version);
                    break;
                }
                Err(e) => return Err(StrimziListError::Api(e)),
            }
        }
    }
    Err(StrimziListError::Refused(format!(
        "the API server serves no KafkaTopic resource ({STRIMZI_GROUP} {}), so the owners of \
         this backup's topics cannot be read; install the Strimzi KafkaTopic CRD or remove \
         spec.strimzi from the KafkaCluster",
        not_served.join(", ")
    )))
}

/// **The selection, pure.** Every resource labelled for the cluster, not
/// unmanaged, whose topic name (`spec.topicName`, else `metadata.name`) is
/// one of `topics`, in name order.
#[must_use]
pub fn select(
    strimzi: &StrimziRef,
    items: &[DynamicObject],
    topics: &[String],
) -> KafkaTopicResourcesInputs {
    let mut resources: Vec<KafkaTopicResourceInputs> = items
        .iter()
        .filter(|o| {
            o.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("strimzi.io/cluster"))
                .is_some_and(|c| *c == strimzi.cluster)
        })
        .filter(|o| {
            o.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("strimzi.io/managed"))
                .is_none_or(|m| m != "false")
        })
        .filter_map(|o| {
            let name = o.metadata.name.clone().filter(|n| !n.is_empty())?;
            let topic_name = o
                .data
                .get("spec")
                .and_then(|s| s.get("topicName"))
                .and_then(serde_json::Value::as_str)
                .filter(|t| !t.is_empty())
                .map(str::to_string);
            let topic = topic_name.clone().unwrap_or_else(|| name.clone());
            topics
                .contains(&topic)
                .then_some(KafkaTopicResourceInputs { name, topic_name })
        })
        .collect();
    resources.sort_by(|a, b| a.name.cmp(&b.name));
    KafkaTopicResourcesInputs {
        cluster: strimzi.cluster.clone(),
        namespace: strimzi.namespace.clone(),
        resources,
    }
}

/// The listed resources as the runner's `--kafka-topic-resources` reads them:
/// one `List` of `KafkaTopic`s, as `kubectl get kafkatopics -o yaml` writes
/// it, rendered from the frozen snapshot alone.
///
/// # Errors
///
/// The serialiser's, for a document that does not serialise.
pub fn document(resources: &KafkaTopicResourcesInputs) -> Result<String, serde_yaml::Error> {
    let items: Vec<serde_json::Value> = resources
        .resources
        .iter()
        .map(|r| {
            let mut spec = serde_json::Map::new();
            if let Some(topic) = &r.topic_name {
                spec.insert("topicName".into(), topic.clone().into());
            }
            serde_json::json!({
                "apiVersion": format!("{STRIMZI_GROUP}/{}", KAFKA_TOPIC_VERSIONS[0]),
                "kind": "KafkaTopic",
                "metadata": {
                    "name": r.name,
                    "namespace": resources.namespace,
                    "labels": {"strimzi.io/cluster": resources.cluster},
                },
                "spec": spec,
            })
        })
        .collect();
    serde_yaml::to_string(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": items,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strimzi(namespace: &str) -> StrimziRef {
        StrimziRef {
            namespace: namespace.into(),
            cluster: "prod-kafka".into(),
        }
    }

    /// KILLS: an unlisted namespace read; a substring or prefix match.
    #[test]
    fn only_a_listed_namespace_is_read() {
        let allowed = configured_namespaces(Some(" kafka, kafka-b ,,"));
        assert_eq!(allowed, ["kafka", "kafka-b"]);
        assert_eq!(admit_namespace(&strimzi("kafka"), &allowed), None);
        assert_eq!(admit_namespace(&strimzi("kafka-b"), &allowed), None);
        for other in ["kube-system", "kafk", "kafka-c", "team-a"] {
            let why = admit_namespace(&strimzi(other), &allowed).expect("refused");
            assert!(why.contains(&format!("`{other}`")), "{why}");
        }
        assert!(
            admit_namespace(&strimzi("kafka"), &configured_namespaces(None))
                .unwrap()
                .contains("lists none")
        );
    }
}
