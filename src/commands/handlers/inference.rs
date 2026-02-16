//! ML Inference command handlers
//!
//! Implements commands for model management and inference:
//! - ML.MODEL.LOAD - Load a model
//! - ML.MODEL.UNLOAD - Unload a model
//! - ML.MODEL.INFO - Get model information
//! - ML.MODEL.LIST - List loaded models
//! - ML.PREDICT - Run inference on a model
//! - ML.PREDICT.BATCH - Run batched inference
//! - ML.TRIGGER.REGISTER - Register inference trigger
//! - ML.TRIGGER.UNREGISTER - Unregister inference trigger
//! - ML.TRIGGER.LIST - List triggers
//! - ML.STATS - Get inference statistics

use std::sync::OnceLock;

use bytes::Bytes;

use super::{err_frame, ok_frame, HandlerContext};
use ferrite_ai::inference::{
    InferenceConfig, InferenceEngine, InferenceInput, InferenceOutput, InferenceTrigger,
    ModelConfig, ModelFormat, TriggerConfig, TriggerOperation,
};
use crate::protocol::Frame;

/// Global inference engine singleton
static INFERENCE_ENGINE: OnceLock<InferenceEngine> = OnceLock::new();

/// Global trigger manager singleton
static TRIGGER_MANAGER: OnceLock<InferenceTrigger> = OnceLock::new();

/// Get or initialize the inference engine
fn get_engine() -> &'static InferenceEngine {
    INFERENCE_ENGINE.get_or_init(|| InferenceEngine::new(InferenceConfig::default()))
}

/// Get or initialize the trigger manager
fn get_trigger_manager() -> &'static InferenceTrigger {
    TRIGGER_MANAGER.get_or_init(InferenceTrigger::new)
}

/// Handle ML.MODEL.LOAD command
///
/// Syntax: ML.MODEL.LOAD model_name path [FORMAT onnx|tflite|torchscript] [GPU] [BATCH max_batch_size]
pub async fn ml_model_load(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("wrong number of arguments for 'ML.MODEL.LOAD' command");
    }

    let model_name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid model name"),
    };

    let path = match std::str::from_utf8(&args[1]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid model path"),
    };

    // Parse optional arguments
    let mut format = ModelFormat::ONNX;
    let mut use_gpu = false;
    let mut max_batch_size = 32;

    let mut i = 2;
    while i < args.len() {
        let arg = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => return err_frame("invalid argument"),
        };

        match arg.as_str() {
            "FORMAT" => {
                i += 1;
                if i >= args.len() {
                    return err_frame("FORMAT requires a value");
                }
                let fmt = match std::str::from_utf8(&args[i]) {
                    Ok(s) => s.to_uppercase(),
                    Err(_) => return err_frame("invalid format value"),
                };
                format = match fmt.as_str() {
                    "ONNX" => ModelFormat::ONNX,
                    "TFLITE" => ModelFormat::TFLite,
                    "TORCHSCRIPT" => ModelFormat::TorchScript,
                    _ => ModelFormat::Custom(fmt),
                };
            }
            "GPU" => {
                use_gpu = true;
            }
            "BATCH" => {
                i += 1;
                if i >= args.len() {
                    return err_frame("BATCH requires a value");
                }
                let batch_str = match std::str::from_utf8(&args[i]) {
                    Ok(s) => s,
                    Err(_) => return err_frame("invalid batch size"),
                };
                max_batch_size = match batch_str.parse() {
                    Ok(v) => v,
                    Err(_) => return err_frame("invalid batch size value"),
                };
            }
            _ => {
                return err_frame(&format!("unknown argument: {}", arg));
            }
        }
        i += 1;
    }

    // Create model config
    let config = ModelConfig {
        name: model_name.to_string(),
        format,
        path: path.to_string(),
        use_gpu,
        max_batch_size,
        ..Default::default()
    };

    // Load the model
    match get_engine().load_model(model_name, config).await {
        Ok(_) => ok_frame(),
        Err(e) => err_frame(&format!("failed to load model: {}", e)),
    }
}

/// Handle ML.MODEL.UNLOAD command
///
/// Syntax: ML.MODEL.UNLOAD model_name
pub async fn ml_model_unload(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'ML.MODEL.UNLOAD' command");
    }

    let model_name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid model name"),
    };

    if get_engine().unload_model(model_name) {
        ok_frame()
    } else {
        err_frame("model not found")
    }
}

/// Handle ML.MODEL.INFO command
///
/// Syntax: ML.MODEL.INFO model_name
pub async fn ml_model_info(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'ML.MODEL.INFO' command");
    }

    let model_name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid model name"),
    };

    match get_engine().model_info(model_name) {
        Some(info) => {
            let mut result = Vec::new();

            result.push(Frame::bulk("name"));
            result.push(Frame::bulk(info.name.clone()));

            result.push(Frame::bulk("version"));
            result.push(Frame::bulk(info.active_version.clone()));

            result.push(Frame::bulk("format"));
            result.push(Frame::bulk(format!("{:?}", info.format)));

            result.push(Frame::bulk("total_predictions"));
            result.push(Frame::Integer(info.total_predictions as i64));

            result.push(Frame::bulk("avg_latency_ms"));
            result.push(Frame::bulk(format!("{:.2}", info.avg_latency_ms)));

            if let Some(ref shape) = info.input_shape {
                result.push(Frame::bulk("input_shape"));
                result.push(Frame::bulk(format!("{:?}", shape)));
            }

            if let Some(ref shape) = info.output_shape {
                result.push(Frame::bulk("output_shape"));
                result.push(Frame::bulk(format!("{:?}", shape)));
            }

            if let Some(ref labels) = info.labels {
                result.push(Frame::bulk("labels"));
                let labels_frame: Vec<Frame> =
                    labels.iter().map(|l| Frame::bulk(l.clone())).collect();
                result.push(Frame::array(labels_frame));
            }

            Frame::array(result)
        }
        None => Frame::Null,
    }
}

/// Handle ML.MODEL.LIST command
///
/// Syntax: ML.MODEL.LIST
pub async fn ml_model_list(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let models = get_engine().list_models();
    let frames: Vec<Frame> = models.into_iter().map(Frame::bulk).collect();
    Frame::array(frames)
}

/// Handle ML.PREDICT command
///
/// Syntax: ML.PREDICT model_name input_type input_value
/// input_type: TEXT, VECTOR, SCALAR, IMAGE
pub async fn ml_predict(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 3 {
        return err_frame("wrong number of arguments for 'ML.PREDICT' command");
    }

    let model_name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid model name"),
    };

    let input_type = match std::str::from_utf8(&args[1]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return err_frame("invalid input type"),
    };

    let input = match input_type.as_str() {
        "TEXT" => {
            let text = match std::str::from_utf8(&args[2]) {
                Ok(s) => s.to_string(),
                Err(_) => return err_frame("invalid text input"),
            };
            InferenceInput::Text(text)
        }
        "VECTOR" => {
            // Parse comma-separated values or JSON array
            let vec_str = match std::str::from_utf8(&args[2]) {
                Ok(s) => s,
                Err(_) => return err_frame("invalid vector input"),
            };
            let values: Result<Vec<f32>, _> = vec_str
                .trim_matches(|c| c == '[' || c == ']')
                .split(',')
                .map(|s| s.trim().parse())
                .collect();
            match values {
                Ok(v) => InferenceInput::Vector(v),
                Err(_) => return err_frame("failed to parse vector values"),
            }
        }
        "SCALAR" => {
            let scalar_str = match std::str::from_utf8(&args[2]) {
                Ok(s) => s,
                Err(_) => return err_frame("invalid scalar input"),
            };
            match scalar_str.parse() {
                Ok(v) => InferenceInput::Scalar(v),
                Err(_) => return err_frame("failed to parse scalar value"),
            }
        }
        "IMAGE" | "RAW" => InferenceInput::Raw(args[2].to_vec()),
        _ => return err_frame("unknown input type (use TEXT, VECTOR, SCALAR, IMAGE, or RAW)"),
    };

    // Run inference
    match get_engine().predict(model_name, input).await {
        Ok(result) => {
            let mut response = Vec::new();

            // Add output based on type
            response.push(Frame::bulk("output"));
            response.push(output_to_frame(&result.output));

            response.push(Frame::bulk("model"));
            response.push(Frame::bulk(result.model));

            response.push(Frame::bulk("latency_ms"));
            response.push(Frame::Integer(result.latency_ms as i64));

            response.push(Frame::bulk("from_cache"));
            response.push(Frame::Integer(if result.from_cache { 1 } else { 0 }));

            Frame::array(response)
        }
        Err(e) => err_frame(&format!("inference failed: {}", e)),
    }
}

/// Handle ML.PREDICT.BATCH command
///
/// Syntax: ML.PREDICT.BATCH model_name input_type value1 [value2 ...]
pub async fn ml_predict_batch(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 3 {
        return err_frame("wrong number of arguments for 'ML.PREDICT.BATCH' command");
    }

    let model_name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid model name"),
    };

    let input_type = match std::str::from_utf8(&args[1]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return err_frame("invalid input type"),
    };

    // Parse all inputs
    let mut inputs = Vec::new();
    for arg in args.iter().skip(2) {
        let input = match input_type.as_str() {
            "TEXT" => {
                let text = match std::str::from_utf8(arg) {
                    Ok(s) => s.to_string(),
                    Err(_) => return err_frame("invalid text input"),
                };
                InferenceInput::Text(text)
            }
            "SCALAR" => {
                let scalar_str = match std::str::from_utf8(arg) {
                    Ok(s) => s,
                    Err(_) => return err_frame("invalid scalar input"),
                };
                match scalar_str.parse() {
                    Ok(v) => InferenceInput::Scalar(v),
                    Err(_) => return err_frame("failed to parse scalar value"),
                }
            }
            _ => return err_frame("batch only supports TEXT and SCALAR input types"),
        };
        inputs.push(input);
    }

    // Run batched inference
    match get_engine().predict_batch(model_name, inputs).await {
        Ok(results) => {
            let frames: Vec<Frame> = results
                .into_iter()
                .map(|r| {
                    let mut result_frame = Vec::new();
                    result_frame.push(Frame::bulk("output"));
                    result_frame.push(output_to_frame(&r.output));
                    result_frame.push(Frame::bulk("from_cache"));
                    result_frame.push(Frame::Integer(if r.from_cache { 1 } else { 0 }));
                    Frame::array(result_frame)
                })
                .collect();
            Frame::array(frames)
        }
        Err(e) => err_frame(&format!("batch inference failed: {}", e)),
    }
}

/// Handle ML.TRIGGER.REGISTER command
///
/// Syntax: ML.TRIGGER.REGISTER name pattern model [INPUT_FIELD field] [OUTPUT_KEY key] [ON op1 op2 ...] [DEBOUNCE ms]
pub async fn ml_trigger_register(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 3 {
        return err_frame("wrong number of arguments for 'ML.TRIGGER.REGISTER' command");
    }

    let name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_string(),
        Err(_) => return err_frame("invalid trigger name"),
    };

    let pattern = match std::str::from_utf8(&args[1]) {
        Ok(s) => s.to_string(),
        Err(_) => return err_frame("invalid pattern"),
    };

    let model = match std::str::from_utf8(&args[2]) {
        Ok(s) => s.to_string(),
        Err(_) => return err_frame("invalid model name"),
    };

    let mut config = TriggerConfig::new(name, pattern, model);

    // Parse optional arguments
    let mut i = 3;
    while i < args.len() {
        let arg = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => return err_frame("invalid argument"),
        };

        match arg.as_str() {
            "INPUT_FIELD" => {
                i += 1;
                if i >= args.len() {
                    return err_frame("INPUT_FIELD requires a value");
                }
                let field = match std::str::from_utf8(&args[i]) {
                    Ok(s) => s.to_string(),
                    Err(_) => return err_frame("invalid input field"),
                };
                config = config.with_input_field(field);
            }
            "OUTPUT_KEY" => {
                i += 1;
                if i >= args.len() {
                    return err_frame("OUTPUT_KEY requires a value");
                }
                let key = match std::str::from_utf8(&args[i]) {
                    Ok(s) => s.to_string(),
                    Err(_) => return err_frame("invalid output key"),
                };
                config = config.with_output_key(key);
            }
            "DEBOUNCE" => {
                i += 1;
                if i >= args.len() {
                    return err_frame("DEBOUNCE requires a value");
                }
                let ms_str = match std::str::from_utf8(&args[i]) {
                    Ok(s) => s,
                    Err(_) => return err_frame("invalid debounce value"),
                };
                let ms: u64 = match ms_str.parse() {
                    Ok(v) => v,
                    Err(_) => return err_frame("invalid debounce milliseconds"),
                };
                config = config.with_debounce(ms);
            }
            "ON" => {
                // Collect all operations until next keyword or end
                let mut operations = Vec::new();
                i += 1;
                while i < args.len() {
                    let op_str = match std::str::from_utf8(&args[i]) {
                        Ok(s) => s.to_uppercase(),
                        Err(_) => break,
                    };
                    let op = match op_str.as_str() {
                        "SET" => TriggerOperation::Set,
                        "UPDATE" => TriggerOperation::Update,
                        "DELETE" => TriggerOperation::Delete,
                        "EXPIRE" => TriggerOperation::Expire,
                        _ => break,
                    };
                    operations.push(op);
                    i += 1;
                }
                if !operations.is_empty() {
                    config.on_operations = operations;
                }
                continue; // Skip the i += 1 at end
            }
            _ => {
                return err_frame(&format!("unknown argument: {}", arg));
            }
        }
        i += 1;
    }

    get_trigger_manager().register(config);
    ok_frame()
}

/// Handle ML.TRIGGER.UNREGISTER command
///
/// Syntax: ML.TRIGGER.UNREGISTER name
pub async fn ml_trigger_unregister(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'ML.TRIGGER.UNREGISTER' command");
    }

    let name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid trigger name"),
    };

    if get_trigger_manager().unregister(name) {
        ok_frame()
    } else {
        err_frame("trigger not found")
    }
}

/// Handle ML.TRIGGER.LIST command
///
/// Syntax: ML.TRIGGER.LIST
pub async fn ml_trigger_list(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let triggers = get_trigger_manager().list();
    let frames: Vec<Frame> = triggers.into_iter().map(Frame::bulk).collect();
    Frame::array(frames)
}

/// Handle ML.TRIGGER.INFO command
///
/// Syntax: ML.TRIGGER.INFO name
pub async fn ml_trigger_info(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'ML.TRIGGER.INFO' command");
    }

    let name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid trigger name"),
    };

    match get_trigger_manager().get(name) {
        Some(config) => {
            let mut result = Vec::new();

            result.push(Frame::bulk("name"));
            result.push(Frame::bulk(config.name.clone()));

            result.push(Frame::bulk("pattern"));
            result.push(Frame::bulk(config.pattern.clone()));

            result.push(Frame::bulk("model"));
            result.push(Frame::bulk(config.model.clone()));

            result.push(Frame::bulk("input_field"));
            result.push(Frame::bulk(config.input_field.clone()));

            result.push(Frame::bulk("enabled"));
            result.push(Frame::Integer(if config.enabled { 1 } else { 0 }));

            result.push(Frame::bulk("debounce_ms"));
            result.push(Frame::Integer(config.debounce_ms as i64));

            if let Some(ref output_key) = config.output_key {
                result.push(Frame::bulk("output_key"));
                result.push(Frame::bulk(output_key.clone()));
            }

            let ops: Vec<Frame> = config
                .on_operations
                .iter()
                .map(|op| Frame::bulk(format!("{:?}", op)))
                .collect();
            result.push(Frame::bulk("on_operations"));
            result.push(Frame::array(ops));

            Frame::array(result)
        }
        None => Frame::Null,
    }
}

/// Handle ML.TRIGGER.ENABLE command
///
/// Syntax: ML.TRIGGER.ENABLE name
pub async fn ml_trigger_enable(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'ML.TRIGGER.ENABLE' command");
    }

    let name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid trigger name"),
    };

    if get_trigger_manager().set_enabled(name, true) {
        ok_frame()
    } else {
        err_frame("trigger not found")
    }
}

/// Handle ML.TRIGGER.DISABLE command
///
/// Syntax: ML.TRIGGER.DISABLE name
pub async fn ml_trigger_disable(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'ML.TRIGGER.DISABLE' command");
    }

    let name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return err_frame("invalid trigger name"),
    };

    if get_trigger_manager().set_enabled(name, false) {
        ok_frame()
    } else {
        err_frame("trigger not found")
    }
}

/// Handle ML.STATS command
///
/// Syntax: ML.STATS
pub async fn ml_stats(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let engine_stats = get_engine().stats();
    let trigger_stats = get_trigger_manager().global_stats();

    let mut result = Vec::new();

    // Engine stats
    result.push(Frame::bulk("total_predictions"));
    result.push(Frame::Integer(engine_stats.total_predictions as i64));

    result.push(Frame::bulk("cache_hit_rate"));
    result.push(Frame::bulk(format!("{:.2}", engine_stats.cache_hit_rate)));

    result.push(Frame::bulk("avg_latency_ms"));
    result.push(Frame::bulk(format!("{:.2}", engine_stats.avg_latency_ms)));

    result.push(Frame::bulk("avg_batch_size"));
    result.push(Frame::bulk(format!("{:.2}", engine_stats.avg_batch_size)));

    result.push(Frame::bulk("models_loaded"));
    result.push(Frame::Integer(engine_stats.models_loaded as i64));

    result.push(Frame::bulk("total_batches"));
    result.push(Frame::Integer(engine_stats.total_batches as i64));

    // Trigger stats
    result.push(Frame::bulk("trigger_events_processed"));
    result.push(Frame::Integer(trigger_stats.events_processed as i64));

    result.push(Frame::bulk("trigger_inferences_run"));
    result.push(Frame::Integer(trigger_stats.inferences_run as i64));

    result.push(Frame::bulk("trigger_failures"));
    result.push(Frame::Integer(trigger_stats.failures as i64));

    Frame::array(result)
}

/// Handle ML.CACHE.CLEAR command
///
/// Syntax: ML.CACHE.CLEAR
pub async fn ml_cache_clear(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    get_engine().clear_cache();
    ok_frame()
}

/// Convert InferenceOutput to Frame
fn output_to_frame(output: &InferenceOutput) -> Frame {
    match output {
        InferenceOutput::Scalar(v) => Frame::bulk(v.to_string()),
        InferenceOutput::Vector(v) => {
            let frames: Vec<Frame> = v.iter().map(|x| Frame::bulk(x.to_string())).collect();
            Frame::array(frames)
        }
        InferenceOutput::Matrix(m) => {
            let frames: Vec<Frame> = m
                .iter()
                .map(|row| {
                    let row_frames: Vec<Frame> =
                        row.iter().map(|x| Frame::bulk(x.to_string())).collect();
                    Frame::array(row_frames)
                })
                .collect();
            Frame::array(frames)
        }
        InferenceOutput::Embedding(v) => {
            let frames: Vec<Frame> = v.iter().map(|x| Frame::bulk(x.to_string())).collect();
            Frame::array(frames)
        }
        InferenceOutput::Classification {
            label,
            confidence,
            all_labels,
        } => {
            let mut result = vec![
                Frame::bulk("label"),
                Frame::bulk(label.clone()),
                Frame::bulk("confidence"),
                Frame::bulk(confidence.to_string()),
                Frame::bulk("all_labels"),
            ];

            let labels: Vec<Frame> = all_labels
                .iter()
                .flat_map(|(k, v)| vec![Frame::bulk(k.clone()), Frame::bulk(v.to_string())])
                .collect();
            result.push(Frame::array(labels));

            Frame::array(result)
        }
        InferenceOutput::Detections(detections) => {
            let frames: Vec<Frame> = detections
                .iter()
                .map(|d| {
                    let det = vec![
                        Frame::bulk("label"),
                        Frame::bulk(d.label.clone()),
                        Frame::bulk("confidence"),
                        Frame::bulk(d.confidence.to_string()),
                        Frame::bulk("bbox"),
                        Frame::array(vec![
                            Frame::bulk(d.bbox[0].to_string()),
                            Frame::bulk(d.bbox[1].to_string()),
                            Frame::bulk(d.bbox[2].to_string()),
                            Frame::bulk(d.bbox[3].to_string()),
                        ]),
                    ];
                    Frame::array(det)
                })
                .collect();
            Frame::array(frames)
        }
        InferenceOutput::Raw(bytes) => Frame::Bulk(Some(Bytes::from(bytes.clone()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_to_frame_scalar() {
        let output = InferenceOutput::Scalar(1.5);
        let frame = output_to_frame(&output);
        assert!(matches!(frame, Frame::Bulk(_)));
    }

    #[test]
    fn test_output_to_frame_vector() {
        let output = InferenceOutput::Vector(vec![1.0, 2.0, 3.0]);
        let frame = output_to_frame(&output);
        if let Frame::Array(Some(arr)) = frame {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_output_to_frame_classification() {
        let mut all_labels = std::collections::HashMap::new();
        all_labels.insert("positive".to_string(), 0.8);
        all_labels.insert("negative".to_string(), 0.2);

        let output = InferenceOutput::Classification {
            label: "positive".to_string(),
            confidence: 0.8,
            all_labels,
        };

        let frame = output_to_frame(&output);
        assert!(matches!(frame, Frame::Array(_)));
    }
}
