Good system design is not about being clever enough to foresee the final shape.
It’s about being patient enough to let the shape reveal itself under pressure.

# Repeatable loop for finding the right abstractions

1. Write something concrete
2. Feel friction
3. Capture it in a test or constraint
4. Name the implicit assumption
5. Extract the smallest possible abstraction
6. Stop

# The key insight for refactoring Storage after introducing Fixed Window algorithm

First proposal to move to a generic storage was:

```go
type Storage interface {
	Get(ctx context.Context, key string, val any) error
	Set(ctx context.Context, key string, val any) error
}
```
this looks more generic, but it actually makes things worse:

1. It hides semantics instead of expressing them. The interface now says: "I store... something"? But the algorithm still knows exactly what it wants.
The knowledge moved into type assertions/abstraction.

2. It pushes correctness from compile time to runtime.

We didn't have **one storage abstraction**

We had **two collapsed layers**:

1. Algorithm state (domain)
2. Persistence mechanism (infrastructure)

When there was one algorithm, collapsing them was fine. With two algorithms, that collapse breaks.
So the fix is not to "make the storage more generic", but to **separate what is stored from how it is stored**.

## Thinking Shift about Abstractions

Don't ask "What interface do I need?"
Ask: "Who owns this concept?"

Abstractions become obvious when ownership is clear.

# Lua Scripts vs Redis Transactions (EXEC/WATCH/MULTI)

Considering that we are rate limiting on a per-user basis, low contention is expected so optimistic locking through Redis transactions is OK.
