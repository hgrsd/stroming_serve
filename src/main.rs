use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use serde_json::Value;
use stroming::{
    MemoryStreamStore, Message, ReadDirection, ReadFromStream, StreamVersion, WriteResult,
    WriteToStream,
};
use warp::{hyper::StatusCode, reply::Reply, Filter};

#[derive(Serialize)]
struct StreamMessageDto {
    pub id: String,
    pub message_type: String,
    pub data: String,
    pub position: String,
    pub revision: String,
}

#[derive(Deserialize)]
struct MessageDto {
    pub message_type: String,
    pub data: Value,
}

#[derive(Deserialize)]
struct WriteStreamDto {
    pub messages: Vec<MessageDto>,
    pub expected_version: String,
}

#[derive(Serialize)]
struct WriteResultResponse {
    pub position: String,
    pub revision: String,
}

#[derive(Serialize)]
struct ReadStreamResponse {
    pub revision: String,
    pub messages: Vec<StreamMessageDto>,
}

#[tokio::main]
async fn main() {
    let store = Arc::new(RwLock::new(MemoryStreamStore::new()));
    let read_s = store.clone();
    let write_s = store.clone();

    let read_stream = warp::get()
        .and(warp::path("stream"))
        .and(warp::path::param())
        .map(move |stream_name: String| {
            let (revision, stream) = read_s
                .read()
                .unwrap()
                .read_from_stream(&stream_name, ReadDirection::Forwards);
            let response = match revision {
                StreamVersion::NoStream => ReadStreamResponse {
                    revision: (-1).to_string(),
                    messages: vec![],
                },
                StreamVersion::Revision(rev) => {
                    let dtos = stream
                        .into_iter()
                        .map(|m| StreamMessageDto {
                            id: m.id.clone(),
                            message_type: m.message_type.clone(),
                            data: String::from_utf8(m.data.clone()).unwrap(),
                            position: m.position.position.to_string(),
                            revision: m.position.revision.to_string(),
                        })
                        .collect();
                    ReadStreamResponse {
                        revision: rev.to_string(),
                        messages: dtos,
                    }
                }
            };
            warp::reply::json(&response)
        });

    let write_stream = warp::post()
        .and(warp::path("stream"))
        .and(warp::path::param())
        .and(warp::body::json())
        .map(move |stream_name: String, write_request: WriteStreamDto| {
            let parsed_ev = write_request.expected_version.parse::<i64>().unwrap();
            let expected_version = if parsed_ev < 0 {
                StreamVersion::NoStream
            } else {
                StreamVersion::Revision(parsed_ev as usize)
            };
            let messages: Vec<Message> = write_request
                .messages
                .into_iter()
                .map(|m| Message {
                    message_type: m.message_type,
                    data: m.data.to_string().as_bytes().to_vec(),
                })
                .collect();
            let append_result =
                write_s
                    .write()
                    .unwrap()
                    .write_to_stream(&stream_name, expected_version, &messages);
            match append_result {
                WriteResult::WrongExpectedVersion => warp::reply::with_status(
                    "Wrong expected version".to_owned(),
                    StatusCode::CONFLICT,
                )
                .into_response(),
                WriteResult::Ok(pos) => {
                    let r = WriteResultResponse {
                        position: pos.position.to_string(),
                        revision: pos.revision.to_string(),
                    };
                    warp::reply::json(&r).into_response()
                }
            }
        });

    warp::serve(read_stream.or(write_stream))
        .run(([127, 0, 0, 1], 3333))
        .await
}
