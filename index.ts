// import { EventEmitter } from "events";
import { v4 as uuid } from "uuid";
import { checkSendArgs } from "./attorney";
import { JobWithMetadata } from "pg-boss";
import {
  SendOptions,
  RequestTuple,
  ConstructorOptionsUpdated,
} from "./attorney";
import { PrismaClient } from "@prisma/client";
import { PrismaPromise } from "@prisma/client";

interface pgBossConfig {
  keepUntil: string;
  expireIn: string;
}

let pgBossConfig: pgBossConfig | undefined = undefined;

pgBossConfig = {
  keepUntil: "14 days",
  expireIn: "15 minutes",
};

const SINGLETON_QUEUE_KEY = "__pgboss__singleton_queue";

class PrismaPGBoss {
  config: ConstructorOptionsUpdated;
  prisma: PrismaClient;

  constructor(config: ConstructorOptionsUpdated, prisma: PrismaClient) {
    // super();
    this.config = config;
    this.prisma = prisma;
  }

  public send(...args: RequestTuple) {
    const { name, data, options } = checkSendArgs(args, this.config);
    return this.createJob(this.prisma, name, data, options);
  }

  public sendOnce(
    name: string,
    data: any,
    options: ConstructorOptionsUpdated,
    key: string
  ) {
    options = options ? { ...options } : {};

    options.singletonKey = key || name;

    const result = checkSendArgs([name, data, options], this.config);

    return this.createJob(
      this.prisma,
      result.name,
      result.data,
      result.options
    );
  }

  public sendSingleton(name: string, data: object, options: SendOptions) {
    options = options ? { ...options } : {};

    options.singletonKey = SINGLETON_QUEUE_KEY;

    const result = checkSendArgs([name, data, options], this.config);

    return this.createJob(
      this.prisma,
      result.name,
      result.data,
      result.options
    );
  }

  public sendAfter(
    name: string,
    data: object,
    options: SendOptions,
    after: string | Date | number | undefined | null
  ) {
    options = options ? { ...options } : {};
    options.startAfter = after;

    const result = checkSendArgs([name, data, options], this.config);

    return this.createJob(
      this.prisma,
      result.name,
      result.data,
      result.options
    );
  }

  public sendThrottled(
    name: string,
    data: object,
    options: SendOptions,
    seconds: number,
    key: string
  ) {
    options = options ? { ...options } : {};
    options.singletonSeconds = seconds;
    options.singletonNextSlot = false;
    options.singletonKey = key;

    const result = checkSendArgs([name, data, options], this.config);

    return this.createJob(
      this.prisma,
      result.name,
      result.data,
      result.options
    );
  }

  public sendDebounced(
    name: string,
    data: object,
    options: SendOptions,
    seconds: number,
    key: string
  ) {
    options = options ? { ...options } : {};
    options.singletonSeconds = seconds;
    options.singletonNextSlot = true;
    options.singletonKey = key;

    const result = checkSendArgs([name, data, options], this.config);

    return this.createJob(
      this.prisma,
      result.name,
      result.data,
      result.options
    );
  }

  public createJob(
    prisma: PrismaClient,
    name: string,
    data: any,
    options: SendOptions | undefined
  ): PrismaPromise<any> {
    let keepUntil = "14 days";
    let expireIn = "15 minutes";
    let singletonOffset = 0;
    const { schema } = this.config || { schema: "pgboss" };

    if (options !== undefined) {
      keepUntil =
        "retentionDays" in options
          ? `${options.retentionDays} days`
          : "retentionHours" in options
          ? `${options.retentionHours} hours`
          : "retentionMinutes" in options
          ? `${options.retentionMinutes} minutes`
          : "retentionSeconds" in options
          ? `${options.retentionSeconds} seconds`
          : pgBossConfig !== undefined
          ? pgBossConfig.keepUntil
          : "14 days";

      expireIn =
        "expireInHours" in options
          ? `${options.expireInHours} hours`
          : "expireInMinutes" in options
          ? `${options.expireInMinutes} minutes`
          : "expireInSeconds" in options
          ? `${options.expireInSeconds} seconds`
          : pgBossConfig !== undefined
          ? pgBossConfig?.expireIn
          : "15 minutes";

      singletonOffset = Number("singletonOffset" in options);
    }

    const {
      priority,
      startAfter,
      singletonKey = null,
      singletonSeconds,
      retryBackoff,
      retryLimit,
      retryDelay,
      onComplete = false,
    } = options || {};

    const id = uuid();

    const values = [
      id, // 1
      name, // 2
      priority, // 3
      retryLimit, // 4
      startAfter, // 5
      expireIn, // 6
      data, // 7
      singletonKey, // 8
      singletonSeconds, // 9
      singletonOffset, // 10
      retryDelay, // 11
      retryBackoff, // 12
      keepUntil, // 13
      onComplete, // 14
    ];

    const states = {
      created: "created",
      retry: "retry",
      active: "active",
      completed: "completed",
      expired: "expired",
      cancelled: "cancelled",
      failed: "failed",
    };

    return prisma.$queryRawUnsafe(
      `
      INSERT INTO ${schema}.job (
        id,
        name,
        priority,
        state,
        retryLimit,
        startAfter,
        expireIn,
        data,
        singletonKey,
        singletonOn,
        retryDelay,
        retryBackoff,
        keepUntil,
        on_complete
      )
      SELECT
        id,
        name,
        priority,
        state,
        retryLimit,
        startAfter,
        expireIn,
        data,
        singletonKey,
        singletonOn,
        retryDelay,
        retryBackoff,
        keepUntil,
        on_complete
      FROM
      ( SELECT *,
          CASE
            WHEN right(keepUntilValue, 1) = 'Z' THEN CAST(keepUntilValue as timestamp with time zone)
            ELSE startAfter + CAST(COALESCE(keepUntilValue,'0') as interval)
            END as keepUntil
        FROM
        ( SELECT *,
            CASE
              WHEN right(startAfterValue, 1) = 'Z' THEN CAST(startAfterValue as timestamp with time zone)
              ELSE now() + CAST(COALESCE(startAfterValue,'0') as interval)
              END as startAfter
          FROM
          ( SELECT
              $1::uuid as id,
              $2::text as name,
              $3::int as priority,
              '${states.created}'::${schema}.job_state as state,
              $4::int as retryLimit,
              $5::text as startAfterValue,
              CAST($6 as interval) as expireIn,
              $7::jsonb as data,
              $8::text as singletonKey,
              CASE
                WHEN $9::integer IS NOT NULL THEN 'epoch'::timestamp + '1 second'::interval * ($9 * floor((date_part('epoch', now()) + $10) / $9))
                ELSE NULL
                END as singletonOn,
              $11::int as retryDelay,
              $12::bool as retryBackoff,
              $13::text as keepUntilValue,
              $14::boolean as on_complete
          ) j1
        ) j2
      ) j3
      ON CONFLICT DO NOTHING
      RETURNING id
    `,
      ...values
    );
  }

  public fetchJobState(
    prisma: PrismaClient,
    jobId: string
  ): PrismaPromise<Pick<JobWithMetadata, "state">[]> {
    const { schema } = this.config || { schema: "pgboss" };

    return prisma.$queryRawUnsafe(
      `SELECT state FROM ${schema}.job WHERE id = '${jobId}'`
    );
  }
}

export default PrismaPGBoss;
