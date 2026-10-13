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
//! **Read only, and opt-in.** The chart grants `get` and `list` on
//! `kafkatopics` in `kafka.strimzi.io`, and nothing else, only with
//! `strimzi.readKafkaTopics: true` (default `false`). This module never
//! creates, changes or deletes one (ADR 0008 Amendment K (4)).
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
/// names one of `topics`, possibly none.
///
/// # Errors
///
/// [`StrimziListError::Refused`] for a 403 or a resource no version of which
/// is served, or more than [`MAX_KAFKA_TOPICS`];
/// [`StrimziListError::Api`] for any other failure.
pub async fn resolve(
    client: &kube::Client,
    cluster: &KafkaCluster,
    topics: &[String],
) -> Result<Option<KafkaTopicResourcesInputs>, StrimziListError> {
    let Some(strimzi) = cluster.spec.strimzi.as_ref() else {
        return Ok(None);
    };
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
                         (403); set the chart value strimzi.readKafkaTopics: true, which grants \
                         get and list on them and nothing else, or remove spec.strimzi from the \
                         KafkaCluster",
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
