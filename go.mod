module github.com/infinivision/filesyncer

require (
	github.com/DataDog/zstd v1.3.4 // indirect
	github.com/Shopify/sarama v1.20.0
	github.com/StackExchange/wmi v0.0.0-20180116203802-5d049714c4a6
	github.com/aws/aws-sdk-go v1.15.88
	github.com/beorn7/perks v0.0.0-20180321164747-3a771d992973
	github.com/boltdb/bolt v1.3.1
	github.com/bouk/monkey v1.0.0
	github.com/bsm/sarama-cluster v2.1.15+incompatible
	github.com/cenkalti/backoff v2.0.0+incompatible
	github.com/cespare/xxhash v1.1.0
	github.com/clbanning/x2j v0.0.0-20161012221638-8514236bc68e
	github.com/coreos/bbolt v1.3.1-coreos.6
	github.com/coreos/etcd v3.3.4+incompatible
	github.com/coreos/go-semver v0.2.0
	github.com/coreos/go-systemd v0.0.0-20181031085051-9002847aa142
	github.com/coreos/pkg v0.0.0-20180108230652-97fdf19511ea
	github.com/davecgh/go-spew v1.1.1
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/dustin/go-humanize v0.0.0-20180421182945-02af3965c54e
	github.com/eapache/go-resiliency v1.1.0
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21
	github.com/eapache/queue v1.1.0
	github.com/fagongzi/goetty v0.0.0-20181130021317-3d2943b977e2
	github.com/fagongzi/log v0.0.0-20170831135209-9a647df25e0e
	github.com/fagongzi/util v0.0.0-20180330021808-4acf02da76a9
	github.com/franela/goreq v0.0.0-20171204163338-bcd34c9993f8
	github.com/ghodss/yaml v1.0.0
	github.com/go-ini/ini v1.37.0
	github.com/go-logfmt/logfmt v0.4.0 // indirect
	github.com/go-ole/go-ole v1.2.1
	github.com/go-redis/redis v6.14.1+incompatible
	github.com/gogo/protobuf v1.2.1
	github.com/golang/protobuf v1.3.1
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c
	github.com/gorilla/websocket v1.4.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.5.1
	github.com/hudl/fargo v0.0.0-20180614092839-fce5cf495554
	github.com/infinivision/hyena v0.0.0-20190212022234-f9f65afd34f5
	github.com/infinivision/prophet v0.0.0-20181120130722-9d348e424ca3
	github.com/jmespath/go-jmespath v0.0.0-20160202185014-0b12d6b521d8
	github.com/jmoiron/sqlx v1.2.0
	github.com/jonboulle/clockwork v0.1.0
	github.com/kisielk/errcheck v1.2.0 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/lib/pq v1.1.0
	github.com/matttproud/golang_protobuf_extensions v1.0.1
	github.com/miekg/dns v1.0.8
	github.com/minio/minio-go v6.0.2+incompatible
	github.com/mitchellh/go-homedir v0.0.0-20180523094522-3864e76763d9
	github.com/montanaflynn/stats v0.5.0
	github.com/op/go-logging v0.0.0-20160315200505-970db520ece7
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/petar/GoLLRB v0.0.0-20130427215148-53be0d36a84c
	github.com/pierrec/lz4 v0.0.0-20181027085611-623b5a2f4d2a
	github.com/pkg/errors v0.8.1
	github.com/pmezard/go-difflib v1.0.0
	github.com/prometheus/client_golang v0.9.2
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90
	github.com/prometheus/common v0.2.0
	github.com/prometheus/procfs v0.0.0-20190403104016-ea9eea638872
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a
	github.com/shirou/gopsutil v0.0.0-20180801053943-8048a2e9c577
	github.com/shopify/sarama v1.20.1-0.20181214121743-94536b3e82d3 // indirect
	github.com/sirupsen/logrus v1.4.1
	github.com/soheilhy/cmux v0.1.4
	github.com/stretchr/testify v1.3.0
	github.com/tmc/grpc-websocket-proxy v0.0.0-20171017195756-830351dc03c6
	github.com/ugorji/go v1.1.1
	github.com/xiang90/probing v0.0.0-20160813154853-07dd2e8dfe18
	github.com/youzan/go-nsq v0.0.0-20180306073406-048121fec907
	golang.org/x/crypto v0.0.0-20190403202508-8e1b8d32e692
	golang.org/x/net v0.0.0-20190403144856-b630fd6fe46b
	golang.org/x/sync v0.0.0-20190227155943-e225da77a7e6 // indirect
	golang.org/x/sys v0.0.0-20190403152447-81d4e9dc473e
	golang.org/x/text v0.3.0
	golang.org/x/time v0.0.0-20180412165947-fbb02b2291d2
	golang.org/x/tools v0.0.0-20190403183509-8a44e74612bc // indirect
	google.golang.org/genproto v0.0.0-20181127195345-31ac5d88444a
	google.golang.org/grpc v1.16.0
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/gcfg.v1 v1.2.3
	gopkg.in/warnings.v0 v0.1.2
	gopkg.in/yaml.v2 v2.2.2
)
