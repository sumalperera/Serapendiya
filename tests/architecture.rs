use std::path::Path;

#[test]
fn workspace_has_expected_entry_points() {
    assert!(Path::new("src/lib.rs").is_file());
    assert!(Path::new("src/bin/streamd.rs").is_file());
    assert!(Path::new("src/bin/streamctl.rs").is_file());
    assert!(!Path::new("src/main.rs").exists());
}
