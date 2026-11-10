//! Verify actual CLI behavior rather than source-file presence.
use std::process::Command;

#[test]
fn entry_points_report_versions() {
    for binary in [
        env!("CARGO_BIN_EXE_streamd"),
        env!("CARGO_BIN_EXE_streamctl"),
    ] {
        let output = Command::new(binary).arg("--version").output().unwrap();
        assert!(output.status.success());
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .contains(env!("CARGO_PKG_VERSION"))
        );
    }
}

#[test]
fn invalid_cli_arguments_fail_and_client_info_is_available() {
    let output = Command::new(env!("CARGO_BIN_EXE_streamd"))
        .arg("--unknown-option")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!output.stderr.is_empty());
    let output = Command::new(env!("CARGO_BIN_EXE_streamctl"))
        .arg("info")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("streamctl")
    );
}
