fn main() {
    if let Err(err) = keel::cli::main() {
        eprintln!("keel: {err}");
        std::process::exit(1);
    }
}
