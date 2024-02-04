import { PrismaClient } from "@prisma/client";
import PrismaPGBoss from "index";
import PgBoss from "pg-boss";
import type { Job } from "pg-boss";
import dotenv from "dotenv";

// PG-Boss is meant to be used as a long running process, so it's might be hard to make a consumer for serverless
// https://github.com/timgit/pg-boss/issues/381#issuecomment-1515708828

// TODO
// Potentially need to override Job interface in our own types.d.ts to add missing attribute expire_in_seconds
//
// interface Job<T> extends PgJob<T> {
//   expire_in_seconds: number;
// }

// Load environment variables from .env file
dotenv.config();

async function someAsyncJobHandler(job: Job<{ message: string }>) {
  console.log("");
  console.log(`job ${job.id} received with data:`);
  console.log(JSON.stringify(job.data));
  console.log("");
}

export function asyncFunctionWithArgsExample({
  depInjection,
}: {
  depInjection: { data: string };
}) {
  return async function (job: Job<{ message: string }>) {
    console.log("----- ");
    console.log("asyncFunctionWithArgsExample");
    console.log(job);
    console.log(job.data.message);
    console.log(depInjection);
    console.log("");
    console.log("");
  };
}

const run = async () => {
  //------------------------------------------------------------------
  // Producer - Enqueing as a client

  const prisma = new PrismaClient();
  const ppgboss = new PrismaPGBoss(
    {
      schema: "pgboss",
    },
    prisma
  );

  // Retrieve the database connection URL from the environment variable
  const boss = new PgBoss(
    process.env.DATABASE_URL ||
      "postgres://postgres@localhost:5432/pgboss?schema=public"
  );

  // @@@@@@@
  // Just to initialize Schema -
  // Needed to create tables once - not needed for producer in regular case.
  await boss.start();
  // @@@@@@@

  await ppgboss.send("test-simple", { message: "no transaction" });

  try {
    await prisma.$transaction([
      ppgboss.send("test2", { message: "YES transaction1" }),
      ppgboss.send("test2", { message: "YES transaction2" }),
      ppgboss.send("test2", { message: "YES transaction3" }),
    ]);
  } catch (err) {
    console.error(err);
  }

  //------------------------------------------------------------------
  // Consumer - Process doing the work
  //
  // await boss.start();

  // await ppgboss.send("reference-email", { email: "kdu3@docs.rutgers.edu" });

  // ----
  // Simplest Case
  await boss.work("test-simple", someAsyncJobHandler);

  // ----
  // Dependency Injection Example
  await boss.work(
    "test2",
    asyncFunctionWithArgsExample({
      depInjection: { data: "thisCouldBeADBClientOrWhatever" },
    })
  );
};

run();
