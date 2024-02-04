# pg-boss-prisma

A way to enqueue jobs in the same transaction as database updates.

For a full example, view [test.ts](./test.ts).

### Enqueuing as a Producer

```typescript
  const prisma = new PrismaClient();
  const ppgboss = new PrismaPGBoss(
    {
      schema: "pgboss",
    },
    prisma
  );

  try {
    await prisma.$transaction([
      prisma.user.create({
       ...
      }),
      ppgboss.send("hello", { message: "YES transaction1" }),
      ppgboss.send("hello", { message: "YES transaction2" }),
      ppgboss.send("hello", { message: "YES transaction3" }),
    ]);
  } catch (err) {
    console.error(err);
  }
```

### Consuming
```typescript
const run = async () => {
  //------------------------------------------------------------------
  // Consumer - Process doing the work
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

  await boss.start();

  console.log("PGBoss Working!");

  // ----
  // Cron jobs
  await boss.schedule("remind-customers", `0 11 * * *`, undefined, {
    tz: "America/New_York",
  });

  // ----
  // Sending Email
  await boss.work("customer-email", customerEmail());
};

run();

```


### Building into as an ESM TS Module Dependency

Depending on how you build your consumer and what dependencies you have, you might need a banner for modules to resolve properly.

`package.json`
```json
  "scripts": {
    "build": "tsup ./index.tsx --format esm",
  },

```

`tsup.config.ts`
```typescript
import { defineConfig } from "tsup";

export default defineConfig((options: Options) => ({
  entry: ["index.ts"],
  dts: true,
  clean: true,
  format: ["esm"],
  esbuildOptions: (options) => {
    options.banner = {
      js: `const require = createRequire(import.meta.url);`,
    };
  },
  ...options,
}));
```
