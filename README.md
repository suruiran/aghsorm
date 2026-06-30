# AghsORM

An SQL ORM upgraded with Aghanim's Shard. But currently, only the SQL building part has been implemented.

# Example

```typescript
class User {
    id: number;
    name: string;
    age: number;
    birthday: Date;
    avatar: Uint8Array;
    is_admin: boolean;
    created_at: Date;
}

const users = new SqlTable<User>({
    schema: "public",
    name: "user",
    fields: [...],
});

users.delete(
    {
        id: ThisCol.gt(Op.plus(1, 2).bracket()),
    }
    {
        orderby: ["age"],
        limit: 10,
    }
).export();

 users.update(
    Op.gte(users.field("id"), 12).and(
        users.equals({ is_admin: true })
    ),
    { age: Op.plus(users.field("age"), 1), created_at: sql`NOW()` },
).export();

users.select(
    Op.gte(users.field("id"), 12).and(
        users.equals({ is_admin: true })
    ),
).export();


sql`select ${11} + ${22} as sum`.export();

```
