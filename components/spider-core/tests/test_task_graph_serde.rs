use spider_core::task::TaskGraph;

const TASK_GRAPH_IN_JSON: &str = r#"{
  "schema_version": "0.1.0",
  "tasks": [
    {
      "tdl_package": "test_pkg",
      "tdl_function": "fn_1",
      "inputs": [
        {
          "Value": {
            "Primitive": {
              "Int": "Int32"
            }
          }
        },
        {
          "Value": {
            "Primitive": {
              "Float": "Float64"
            }
          }
        }
      ],
      "outputs": [
        {
          "Value": {
            "Primitive": {
              "Int": "Int64"
            }
          }
        },
        {
          "Value": {
            "Primitive": "Boolean"
          }
        }
      ],
      "input_sources": null
    },
    {
      "tdl_package": "test_pkg",
      "tdl_function": "fn_2",
      "inputs": [
        {
          "Value": {
            "Bytes": {}
          }
        }
      ],
      "outputs": [
        {
          "Value": {
            "List": {
              "Primitive": {
                "Int": "Int32"
              }
            }
          }
        },
        {
          "Value": {
            "Bytes": {}
          }
        }
      ],
      "input_sources": null
    },
    {
      "tdl_package": "test_pkg",
      "tdl_function": "fn_3",
      "inputs": [
        {
          "Value": {
            "Primitive": {
              "Int": "Int64"
            }
          }
        }
      ],
      "outputs": [
        {
          "Value": {
            "Map": {
              "key": {
                "Int": "Int32"
              },
              "value": {
                "Primitive": {
                  "Float": "Float64"
                }
              }
            }
          }
        },
        {
          "Value": {
            "Struct": "Result"
          }
        }
      ],
      "input_sources": [
        {
          "task_idx": 0,
          "position": 0
        }
      ]
    },
    {
      "tdl_package": "test_pkg",
      "tdl_function": "fn_4",
      "inputs": [
        {
          "Value": {
            "Map": {
              "key": {
                "Int": "Int32"
              },
              "value": {
                "Primitive": {
                  "Float": "Float64"
                }
              }
            }
          }
        },
        {
          "Value": {
            "Primitive": "Boolean"
          }
        }
      ],
      "outputs": [
        {
          "Value": {
            "Primitive": {
              "Int": "Int32"
            }
          }
        }
      ],
      "input_sources": [
        {
          "task_idx": 2,
          "position": 0
        },
        {
          "task_idx": 0,
          "position": 1
        }
      ]
    },
    {
      "tdl_package": "test_pkg",
      "tdl_function": "fn_5",
      "inputs": [
        {
          "Value": {
            "Map": {
              "key": {
                "Int": "Int32"
              },
              "value": {
                "Primitive": {
                  "Float": "Float64"
                }
              }
            }
          }
        },
        {
          "Value": {
            "List": {
              "Primitive": {
                "Int": "Int32"
              }
            }
          }
        }
      ],
      "outputs": [
        {
          "Value": {
            "Primitive": {
              "Float": "Float32"
            }
          }
        },
        {
          "Value": {
            "Bytes": {}
          }
        }
      ],
      "input_sources": [
        {
          "task_idx": 2,
          "position": 0
        },
        {
          "task_idx": 1,
          "position": 0
        }
      ]
    },
    {
      "tdl_package": "test_pkg",
      "tdl_function": "fn_6",
      "inputs": [
        {
          "Value": {
            "Primitive": {
              "Int": "Int32"
            }
          }
        }
      ],
      "outputs": [
        {
          "Value": {
            "Primitive": "Boolean"
          }
        },
        {
          "Value": {
            "List": {
              "Bytes": {}
            }
          }
        }
      ],
      "input_sources": [
        {
          "task_idx": 3,
          "position": 0
        }
      ]
    },
    {
      "tdl_package": "test_pkg",
      "tdl_function": "fn_7",
      "inputs": [
        {
          "Value": {
            "List": {
              "Bytes": {}
            }
          }
        },
        {
          "Value": {
            "List": {
              "Bytes": {}
            }
          }
        },
        {
          "Value": {
            "Primitive": "Boolean"
          }
        },
        {
          "Value": {
            "Bytes": {}
          }
        }
      ],
      "outputs": [
        {
          "Value": {
            "Primitive": {
              "Int": "Int64"
            }
          }
        }
      ],
      "input_sources": [
        {
          "task_idx": 5,
          "position": 1
        },
        {
          "task_idx": 5,
          "position": 1
        },
        {
          "task_idx": 5,
          "position": 0
        },
        {
          "task_idx": 4,
          "position": 1
        }
      ]
    },
    {
      "tdl_package": "test_pkg",
      "tdl_function": "fn_8",
      "inputs": [
        {
          "Value": {
            "List": {
              "Bytes": {}
            }
          }
        }
      ],
      "outputs": [
        {
          "Value": {
            "Primitive": {
              "Float": "Float64"
            }
          }
        }
      ],
      "input_sources": [
        {
          "task_idx": 5,
          "position": 1
        }
      ]
    },
    {
      "tdl_package": "test_pkg",
      "tdl_function": "fn_9",
      "inputs": [
        {
          "Value": {
            "Bytes": {}
          }
        },
        {
          "Value": {
            "List": {
              "Primitive": {
                "Int": "Int32"
              }
            }
          }
        }
      ],
      "outputs": [
        {
          "Value": {
            "Primitive": {
              "Int": "Int32"
            }
          }
        }
      ],
      "input_sources": [
        {
          "task_idx": 1,
          "position": 1
        },
        {
          "task_idx": 1,
          "position": 0
        }
      ]
    },
    {
      "tdl_package": "test_pkg",
      "tdl_function": "fn_10",
      "inputs": [],
      "outputs": [],
      "input_sources": null
    }
  ]
}"#;

#[test]
fn test_serde() {
    let task_graph =
        TaskGraph::from_json(TASK_GRAPH_IN_JSON).expect("deserialization from JSON should succeed");
    let serialized_task_graph = task_graph
        .to_json()
        .expect("serialization to JSON should succeed");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(TASK_GRAPH_IN_JSON)
            .expect("deserialization from JSON should succeed"),
        serde_json::from_str::<serde_json::Value>(&serialized_task_graph)
            .expect("deserialization from JSON should succeed")
    );
    let deserialized_task_graph: TaskGraph = TaskGraph::from_json(&serialized_task_graph)
        .expect("deserialization from JSON should succeed");
    assert_eq!(task_graph, deserialized_task_graph);
}
