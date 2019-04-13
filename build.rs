fn main() {
    prost_build::compile_protos(&["src/arbiter.proto"],
                                &["src/"]).unwrap();
}