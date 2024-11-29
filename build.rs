fn main() {
    protobuf_codegen::Codegen::new()
        .protoc()
        .cargo_out_dir("generated")
        .input("src/protos/chunk.proto")
        .input("src/protos/operations.proto")
        .input("src/protos/schema.proto")
        .include("src/protos")
        .run_from_script();
}
