# Signing a user in against a store

A flow can keep users in a typed store and check their passwords with two FEEL builtins,
`password hash` and `password verify` (new in 0.6.21; reference in
[Rules: DMN, FEEL, and .srl](rules.md#password-hashing)). This walkthrough seeds one user from a migration, enrols a
second through a flow, and signs both in. The password itself is never stored.

You need the engine and PostgreSQL. This walkthrough was run on PostgreSQL 16, with the output
shown below.

## The two builtins

| Function | Returns |
|---|---|
| `password hash(password)` | An argon2id hash in PHC string form, `$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>`, with a fresh 16-byte salt from the OS. `null` if the argument is not a string or is empty. |
| `password verify(password, hash)` | `true` if the password matches the hash, `false` if it does not. `null` if either argument is not a string, or if the hash is malformed, is not argon2id, or asks for more than m = 65 536 KiB, t = 10 or p = 4. The comparison is constant-time. |

The parameters are fixed: argon2id, m = 19 456 KiB, t = 2, p = 1, the OWASP minimum. A package
can't change them. `verify` reads the parameters from the stored hash, so the cap keeps a planted
hash from tying up a worker and its memory.

`password hash` draws a random salt, so it is denied at replay-bound sites, like `uuid()`. Use it
in a task, not in an alias or a `when`. `password verify` is pure and works anywhere.

FEEL evaluation blocks its executor worker, so one verify holds a worker for one hash time, about
22 ms on a 2020 laptop CPU (Intel i7-10750H). That is fine for sign-ins. Don't call these functions per message.

## 1. A typed store for users

`package.yaml` labels the module `signin`, version `1.0.0`. `datastores.yaml` gives the store its
row type:

```yaml
datastores:
  - name: users
    type: sql
    structure:
      schema: urn:user-row
      type: UserRow
    sql:
      url-ref: env:STORE_DB_URL
      username-ref: env:STORE_DB_USER
      password-ref: env:STORE_DB_PASSWORD
      migrations: migrations/users
```

`schemas/user-row/codec-manifest.yaml` is `schemaKind: xsd` and `formats: [xml]`.
`schemas/user-row/user-row.xsd` declares the row:

```xml
<xs:element name="UserRow">
  <xs:complexType>
    <xs:sequence>
      <xs:element name="userId"       type="UserId"/>   <!-- maxLength 64 -->
      <xs:element name="passwordHash" type="PhcHash"/>  <!-- maxLength 256 -->
    </xs:sequence>
  </xs:complexType>
</xs:element>
```

`migrations/users/V001__users.sql` creates the table and seeds one user, `ada`. The hash is a fixed
test vector for the password `correct horse`:

```sql
CREATE TABLE IF NOT EXISTS users (
  store_key     VARCHAR(512) NOT NULL,
  user_id       VARCHAR(64)  NOT NULL,
  password_hash VARCHAR(256) NOT NULL,
  rev           BIGINT       NOT NULL DEFAULT 1,
  updated_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (store_key)
);
INSERT INTO users (store_key, user_id, password_hash)
VALUES ('ada', 'ada',
        '$argon2id$v=19$m=19456,t=2,p=1$AAECAwQFBgcICQoLDA0ODw$0AuQxA+RjPiJeA19Op7DwrFxnfh9xeTU2T6mFj2wThc')
ON CONFLICT (store_key) DO NOTHING;
```

## 2. Two channels

`channels.yaml` declares two JSON channels with the usual API-key auth. Both take
`{"user": ..., "password": ...}`:

```yaml
channels:
  - name: enrol
    transport: http
    bind: "POST /channels/enrol"
    codec: json
    cloudevents-mode: none
    auth: { scheme: apikey, apikey: { value: env:SIGN_IN_API_KEY, header: X-Api-Key } }
  - name: sign-in
    transport: http
    bind: "POST /channels/sign-in"
    codec: json
    cloudevents-mode: none
    auth: { scheme: apikey, apikey: { value: env:SIGN_IN_API_KEY, header: X-Api-Key } }
```

## 3. Keep the password out of storage

Both flows bind the request to a variable named `creds` and declare it `transient` and `sensitive`.
`transient` keeps it out of the instance store. `sensitive` redacts it, and the raw request body
with it, in logs and audit. Neither flow waits, so `creds` is only needed for one turn.

`bpmn/enrol.bpmn` hashes the password and writes the row:

```xml
<bpmn:dataStore id="usersStore" name="users"/>
<bpmn:process id="enrol" isExecutable="true">
  <bpmn:extensionElements>
    <q:audit sink="jsonl" capture="payload"/>
    <q:variables>
      <q:variable name="creds" type="object" transient="true" sensitive="true"/>
      <q:variable name="row" type="object"/>
    </q:variables>
  </bpmn:extensionElements>
  <bpmn:dataStoreReference id="refUser" dataStoreRef="usersStore">
    <bpmn:extensionElements><q:store key="creds.user"/></bpmn:extensionElements>
  </bpmn:dataStoreReference>
  <bpmn:startEvent id="Start">
    <bpmn:extensionElements><q:source channel="enrol" name="creds"/></bpmn:extensionElements>
  </bpmn:startEvent>
  <bpmn:serviceTask id="Hash" name="Hash and store">
    <bpmn:dataInputAssociation>
      <bpmn:assignment>
        <bpmn:from>{userId: creds.user, passwordHash: password hash(creds.password)}</bpmn:from>
        <bpmn:to>row</bpmn:to>
      </bpmn:assignment>
    </bpmn:dataInputAssociation>
    <bpmn:dataOutputAssociation>
      <bpmn:sourceRef>row</bpmn:sourceRef><bpmn:targetRef>refUser</bpmn:targetRef>
    </bpmn:dataOutputAssociation>
  </bpmn:serviceTask>
  <bpmn:endEvent id="Done"/>
  <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="Hash"/>
  <bpmn:sequenceFlow id="F2" sourceRef="Hash" targetRef="Done"/>
</bpmn:process>
```

`bpmn/sign-in.bpmn` reads the row, checks the password, and answers:

```xml
<bpmn:dataStore id="usersStore" name="users"/>
<bpmn:process id="sign-in" isExecutable="true">
  <bpmn:extensionElements>
    <q:audit sink="jsonl" capture="payload"/>
    <q:variables>
      <q:variable name="creds" type="object" transient="true" sensitive="true"/>
      <q:variable name="user" type="object"/>
      <q:variable name="signedIn" type="boolean"/>
    </q:variables>
  </bpmn:extensionElements>
  <bpmn:dataStoreReference id="refUser" dataStoreRef="usersStore">
    <bpmn:extensionElements><q:store key="creds.user"/></bpmn:extensionElements>
  </bpmn:dataStoreReference>
  <bpmn:startEvent id="Start">
    <bpmn:extensionElements><q:source channel="sign-in" name="creds"/></bpmn:extensionElements>
  </bpmn:startEvent>
  <bpmn:serviceTask id="Load" name="Read the user">
    <bpmn:dataInputAssociation>
      <bpmn:sourceRef>refUser</bpmn:sourceRef><bpmn:targetRef>user</bpmn:targetRef>
    </bpmn:dataInputAssociation>
  </bpmn:serviceTask>
  <bpmn:serviceTask id="Check" name="Check the password">
    <bpmn:dataInputAssociation>
      <bpmn:assignment>
        <bpmn:from>password verify(creds.password, user.passwordHash) = true</bpmn:from>
        <bpmn:to>signedIn</bpmn:to>
      </bpmn:assignment>
    </bpmn:dataInputAssociation>
  </bpmn:serviceTask>
  <bpmn:serviceTask id="Answer" implementation="signed-in.hbs">
    <bpmn:extensionElements><q:reply mode="native" contentType="application/json"/></bpmn:extensionElements>
  </bpmn:serviceTask>
  <bpmn:endEvent id="Done"/>
  <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="Load"/>
  <bpmn:sequenceFlow id="F2" sourceRef="Load" targetRef="Check"/>
  <bpmn:sequenceFlow id="F3" sourceRef="Check" targetRef="Answer"/>
  <bpmn:sequenceFlow id="F4" sourceRef="Answer" targetRef="Done"/>
</bpmn:process>
```

The `= true` turns a `null` from `password verify` (a malformed stored hash) into `false`, so the
answer is always a boolean.

`templates/signed-in.hbs`:

```text
{"signedIn":{{signedIn}}}
```

## 4. Package it and start the engine

```bash
sutra lint signin
sutra package signin -o deployments
```

Give the engine a role that can't bypass row-level security, and a database that role owns:

```sql
CREATE ROLE sutra_app LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '<a password>';
CREATE DATABASE sutra OWNER sutra_app;
```

Start the engine with `SUTRA_DATASOURCE_URL` pointing at that database as `sutra_app`,
`SUTRA_DEPLOYMENTS_DIR` pointing at `deployments`, `SIGN_IN_API_KEY`, `STORE_DB_URL`, `STORE_DB_USER`
and `STORE_DB_PASSWORD` for the store, and `SUTRA_AUDIT_JSONL` for the audit file. Set `B` to the
engine's address.

## 5. Sign in the seeded user

```bash
curl -s -X POST "$B/channels/sign-in" -H 'X-Api-Key: <your key>' \
  -H 'Content-Type: application/json' -d '{"user":"ada","password":"correct horse"}'
```

```json
{"signedIn":true}
```

With `"password":"correct horsf"` instead:

```json
{"signedIn":false}
```

## 6. Enrol a user

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST "$B/channels/enrol" -H 'X-Api-Key: <your key>' \
  -H 'Content-Type: application/json' -d '{"user":"grace","password":"<a password>"}'
```

```text
200
```

The row holds the hash, with its own salt:

```sql
SELECT store_key, user_id, password_hash FROM users ORDER BY store_key;
```

```text
 store_key | user_id |                                           password_hash
-----------+---------+---------------------------------------------------------------------------------------------------
 ada       | ada     | $argon2id$v=19$m=19456,t=2,p=1$AAECAwQFBgcICQoLDA0ODw$0AuQxA+RjPiJeA19Op7DwrFxnfh9xeTU2T6mFj2wThc
 grace     | grace   | $argon2id$v=19$m=19456,t=2,p=1$egxSk+lhFOUDspiuABfQoQ$QzDs4/To+3jKTJclLpfm3cpGF1N41U2Ig4fZuXsTn3M
```

Signing `grace` in with that password answers `{"signedIn":true}`, and with any other password
`{"signedIn":false}`.

## 7. Where the password went

Nowhere. A data-only `pg_dump` of the engine's database has no row containing either password, and
neither does the audit file or the engine log. The audit file records each step, with `creds` and
the raw body redacted:

```json
{"eventType":"NODE_ENTERED","nodeId":"Answer","payload":{"creds":"***REDACTED***","event":{"body":"[REDACTED]", ...},"signedIn":true, ...}, ...}
```

## 8. Not at a replay-bound site

Put `password hash` in an alias and lint refuses the package:

```xml
<q:alias name="h" expression="password hash(creds.password)"/>
```

```text
[ERROR] SUTRA.FEEL.DETERMINISM.UNSAFE_BUILTIN — ... The FEEL expression on process 'sign-in' node 'Start' alias 'h' uses `password hash`, which is denied at replay-bound sites.
```

## Limits

- **Waits.** `creds` is `transient`, so it is gone after a wait, and lint refuses a read of it after
  one. Check the password before any wait.
- **Cost.** Each call costs one argon2id hash on an executor worker. Rate-limit the sign-in channel
  if it is exposed.
- **Unknown users.** This flow assumes the row exists. A real one should answer `false` for an
  unknown user too, after the same amount of work, so the answer time doesn't reveal which users
  exist.
