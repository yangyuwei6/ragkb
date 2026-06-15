FROM docker.io/library/elasticsearch:8.13.4

# IK analyzer 必须和 Elasticsearch 小版本一致，否则 ES 启动时会拒绝加载插件。
RUN elasticsearch-plugin install --batch https://get.infini.cloud/elasticsearch/analysis-ik/8.13.4
