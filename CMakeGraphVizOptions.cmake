# CMakeGraphVizOptions.cmake

set(GRAPHVIZ_EXECUTABLES     TRUE)
set(GRAPHVIZ_STATIC_LIBS     TRUE)
set(GRAPHVIZ_SHARED_LIBS     TRUE)
set(GRAPHVIZ_MODULE_LIBS     TRUE)
set(GRAPHVIZ_INTERFACE_LIBS  FALSE)

# Won't hide vendored deps, but keep it anyway:
set(GRAPHVIZ_EXTERNAL_LIBS   FALSE)

# Important: per-target files are not what you want; this explodes graphs.
set(GRAPHVIZ_GENERATE_PER_TARGET FALSE)

# Ignore targets by *target name*
set(GRAPHVIZ_IGNORE_TARGETS
  # Abseil: match alias targets if present
  "^absl::.*$"

  # Abseil when built from source: these are often *plain* target names.
  # This list is based directly on what appears before the \n in your .dot.
  "^cord$"
  "^base$"
  "^log_severity$"
  "^raw_logging_internal$"
  "^spinlock_wait$"
  "^cord_internal$"
  "^crc_cord_state$"
  "^crc32c$"
  "^crc_cpu_detect$"
  "^bad_optional_access$"
  "^crc_internal$"
  "^throw_delegate$"
  "^str_format_internal$"
  "^int128$"
  "^strings$"
  "^string_view$"
  "^strings_internal$"
  "^debugging_internal$"
  "^cordz_functions$"
  "^exponential_biased$"
  "^cordz_info$"
  "^cordz_handle$"
  "^synchronization$"
  "^graphcycles_internal$"
  "^malloc_internal$"
  "^kernel_timeout_internal$"
  "^time$"
  "^civil_time$"
  "^time_zone$"
  "^stacktrace$"
  "^symbolize$"
  "^demangle_internal$"
  "^demangle_rust$"
  "^decode_rust_punycode$"
  "^utf8_for_code_point$"
  "^tracing_internal$"
  "^hash$"
  "^city$"
  "^low_level_hash$"
  "^bad_variant_access$"
  "^raw_hash_set$"
  "^hashtablez_sampler$"
  "^bad_any_cast_impl$"
  "^cordz_sample_token$"
  "^failure_signal_handler$"
  "^flags_.*$"
  "^log_.*$"
  "^periodic_sampler$"
  "^poison$"
  "^random_.*$"
  "^status$"
  "^statusor$"
  "^strerror$"
  "^die_if_null$"
  "^scoped_set_env$"
  "^leak_check$"
  "^examine_stack$"
  "^vlog_config_internal$"

  # other 3rd party
  "^spdlog.*$"
  "^asio.*$"
  "^jwt-cpp.*$"
  "^secp256k1.*$"
  "^evmc.*$"
  "^evmone.*$"
  "^nlohmann_json.*$"
  "^protobuf.*$"
  "^libprotobuf.*$"
  "^protoc.*$"
  "^libprotoc.*$"
  "^libupb$"
  "^utf8_validity$"
  "^utf8_range$"
  "^instructions$"
  "^loader$"
  "^tooling$"
  "^gtest.*$"
  "^gmock.*$"
)