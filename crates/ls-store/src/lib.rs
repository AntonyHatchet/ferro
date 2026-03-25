use dashmap::DashMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StoreKey {
    pub account_id: String,
    pub region: String,
}

pub trait Store: Default + Send + Sync + 'static {
    fn service_name() -> &'static str;
}

pub struct AccountRegionBundle<S: Store> {
    service_name: &'static str,
    stores: Arc<DashMap<StoreKey, Arc<S>>>,
    global: Arc<DashMap<String, serde_json::Value>>,
}

impl<S: Store> AccountRegionBundle<S> {
    pub fn new() -> Self {
        Self {
            service_name: S::service_name(),
            stores: Arc::new(DashMap::new()),
            global: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, account_id: &str, region: &str) -> Arc<S> {
        let key = StoreKey {
            account_id: account_id.to_string(),
            region: region.to_string(),
        };
        self.stores
            .entry(key)
            .or_insert_with(|| Arc::new(S::default()))
            .value()
            .clone()
    }

    pub fn service_name(&self) -> &'static str {
        self.service_name
    }

    pub fn reset(&self) {
        self.stores.clear();
        self.global.clear();
    }

    pub fn iter_stores(&self) -> Vec<(String, String, Arc<S>)> {
        self.stores
            .iter()
            .map(|entry| {
                (
                    entry.key().account_id.clone(),
                    entry.key().region.clone(),
                    entry.value().clone(),
                )
            })
            .collect()
    }

    pub fn global(&self) -> &DashMap<String, serde_json::Value> {
        &self.global
    }
}

impl<S: Store> Default for AccountRegionBundle<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Store> Clone for AccountRegionBundle<S> {
    fn clone(&self) -> Self {
        Self {
            service_name: self.service_name,
            stores: Arc::clone(&self.stores),
            global: Arc::clone(&self.global),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct TestStore {
        pub counter: std::sync::atomic::AtomicU32,
    }

    impl Store for TestStore {
        fn service_name() -> &'static str {
            "test"
        }
    }

    #[test]
    fn get_creates_store_on_first_access() {
        let bundle = AccountRegionBundle::<TestStore>::new();
        let store = bundle.get("123456789012", "us-east-1");
        store.counter.store(42, std::sync::atomic::Ordering::SeqCst);

        let same = bundle.get("123456789012", "us-east-1");
        assert_eq!(same.counter.load(std::sync::atomic::Ordering::SeqCst), 42);
    }

    #[test]
    fn different_accounts_get_different_stores() {
        let bundle = AccountRegionBundle::<TestStore>::new();
        let a = bundle.get("111111111111", "us-east-1");
        let b = bundle.get("222222222222", "us-east-1");
        a.counter.store(1, std::sync::atomic::Ordering::SeqCst);
        b.counter.store(2, std::sync::atomic::Ordering::SeqCst);

        assert_eq!(
            bundle.get("111111111111", "us-east-1").counter.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            bundle.get("222222222222", "us-east-1").counter.load(std::sync::atomic::Ordering::SeqCst),
            2
        );
    }

    #[test]
    fn different_regions_get_different_stores() {
        let bundle = AccountRegionBundle::<TestStore>::new();
        bundle.get("123456789012", "us-east-1").counter.store(10, std::sync::atomic::Ordering::SeqCst);
        bundle.get("123456789012", "eu-west-1").counter.store(20, std::sync::atomic::Ordering::SeqCst);

        assert_eq!(
            bundle.get("123456789012", "us-east-1").counter.load(std::sync::atomic::Ordering::SeqCst),
            10
        );
        assert_eq!(
            bundle.get("123456789012", "eu-west-1").counter.load(std::sync::atomic::Ordering::SeqCst),
            20
        );
    }

    #[test]
    fn clone_shares_state() {
        let bundle = AccountRegionBundle::<TestStore>::new();
        bundle.get("123456789012", "us-east-1").counter.store(99, std::sync::atomic::Ordering::SeqCst);

        let cloned = bundle.clone();
        assert_eq!(
            cloned.get("123456789012", "us-east-1").counter.load(std::sync::atomic::Ordering::SeqCst),
            99
        );
    }

    #[test]
    fn reset_clears_all_stores() {
        let bundle = AccountRegionBundle::<TestStore>::new();
        bundle.get("123456789012", "us-east-1");
        bundle.get("123456789012", "eu-west-1");
        assert_eq!(bundle.iter_stores().len(), 2);

        bundle.reset();
        assert_eq!(bundle.iter_stores().len(), 0);
    }

    #[test]
    fn service_name_returns_correct_value() {
        let bundle = AccountRegionBundle::<TestStore>::new();
        assert_eq!(bundle.service_name(), "test");
    }

    #[test]
    fn iter_stores_returns_all_entries() {
        let bundle = AccountRegionBundle::<TestStore>::new();
        bundle.get("111111111111", "us-east-1");
        bundle.get("222222222222", "eu-west-1");
        let entries = bundle.iter_stores();
        assert_eq!(entries.len(), 2);
        let accounts: Vec<String> = entries.iter().map(|(a, _, _)| a.clone()).collect();
        assert!(accounts.contains(&"111111111111".to_string()));
        assert!(accounts.contains(&"222222222222".to_string()));
    }
}
