package config

// Config 是整个应用的配置总入口。
// 配置只从 yaml 文件读取；本地真实配置文件不提交到 git。
type Config struct {
	HTTP   HTTPConfig   `yaml:"http"`
	MySQL  MySQLConfig  `yaml:"mysql"`
	Redis  RedisConfig  `yaml:"redis"`
	ES     ESConfig     `yaml:"elasticsearch"`
	Qdrant QdrantConfig `yaml:"qdrant"`
	MinIO  MinIOConfig  `yaml:"minio"`
	Kafka  KafkaConfig  `yaml:"kafka"`
	Tika   TikaConfig   `yaml:"tika"`
	AI     AIConfig     `yaml:"ai"`
	JWT    JWTConfig    `yaml:"jwt"`
	Upload UploadConfig `yaml:"upload"`
	Search SearchConfig `yaml:"search"`
	Log    LogConfig    `yaml:"log"`
}

type HTTPConfig struct {
	Addr string `yaml:"addr"`
}

type MySQLConfig struct {
	DSN string `yaml:"dsn"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type ESConfig struct {
	URL       string `yaml:"url"`
	IndexName string `yaml:"index_name"`
}

type QdrantConfig struct {
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	Collection string `yaml:"collection"`
}

type MinIOConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Bucket    string `yaml:"bucket"`
	UseSSL    bool   `yaml:"use_ssl"`
}

type KafkaConfig struct {
	Brokers  []string `yaml:"brokers"`
	Topic    string   `yaml:"topic"`
	DLQTopic string   `yaml:"dlq_topic"`
	GroupID  string   `yaml:"group_id"`
}

type TikaConfig struct {
	URL string `yaml:"url"`
}

type AIConfig struct {
	LLMBaseURL           string `yaml:"llm_base_url"`
	LLMModel             string `yaml:"llm_model"`
	LLMAPIKey            string `yaml:"llm_api_key"`
	EmbeddingBaseURL     string `yaml:"embedding_base_url"`
	EmbeddingModel       string `yaml:"embedding_model"`
	EmbeddingAPIKey      string `yaml:"embedding_api_key"`
	EmbeddingDimensions  int    `yaml:"embedding_dimensions"`
	EmbeddingBatchSize   int    `yaml:"embedding_batch_size"`
	EmbeddingConcurrency int    `yaml:"embedding_concurrency"`
	RerankBaseURL        string `yaml:"rerank_base_url"`
	RerankModel          string `yaml:"rerank_model"`
	RerankAPIKey         string `yaml:"rerank_api_key"`
}

type JWTConfig struct {
	Secret             string `yaml:"secret"`
	ExpireHours        int    `yaml:"expire_hours"`
	RefreshExpireHours int    `yaml:"refresh_expire_hours"`
}

// UploadConfig 控制分片上传的安全约束。
type UploadConfig struct {
	// MaxFileSizeMB 单文件大小上限（MB）。
	MaxFileSizeMB int `yaml:"max_file_size_mb"`
	// AllowedExts 允许上传的扩展名（小写，不含点）。为空时用默认白名单。
	AllowedExts []string `yaml:"allowed_exts"`
}

type SearchConfig struct {
	TopK            int `yaml:"top_k"`
	RecallK         int `yaml:"recall_k"`
	RRFK            int `yaml:"rrf_k"`
	RerankTimeoutMS int `yaml:"rerank_timeout_ms"`
}

type LogConfig struct {
	Level string `yaml:"level"`
	// Mode 为 dev 时输出带颜色的人类可读日志，prod 时输出 JSON。
	Mode string `yaml:"mode"`
}
