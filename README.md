# avro-kafka0.10
刚项目主要用于测试用途，avro序列化和反序列化

{
    "namespace": "com.avro.example",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number",  "type": ["int", "null"]},
        {"name": "favorite_color", "type": ["string", "null"]}
    ]
}

java -jar avro-tools-1.8.2.jar compile schema driverposition.avsc . 生成java文件
