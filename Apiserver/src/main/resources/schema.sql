create table if not exists ConsumerStatus
(
    bootstrapServersConfig      varchar(10000),
    autoCommitConfig            integer default 0,
    groupIdConfig               varchar(500),
    buffTopicName               varchar(10000),
    buffMinBatchSize            integer,
    buffIsHashListInclude       integer default 0,
    buffTimeoutMillis           integer,
    buffPollIntervalMillis      integer,
    immediateTopicName          varchar(10000),
    immediateIsHashListInclude  integer default 0,
    immediateTimeoutMillis      integer,
    immediatePollIntervalMillis integer,
    consumerType integer
);


