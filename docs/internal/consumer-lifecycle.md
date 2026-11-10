# Consumer cancellation and the two consumer lifetimes

How `pika/channel.py` and the `BlockingChannel` wrapper in `pika/adapters/blocking_connection.py` move a consumer from registration through cancellation to retirement, and what keeps a consumer tag that is reused after cancellation from mixing the two consumers up.

## The bookkeeping and its single retirement transition

`Channel` keeps two structures. `_consumers` maps a consumer tag to its delivery callback. `_cancelled` is the set of tags whose cancel has been sent but not retired, so `basic_consume` refuses to reuse one while a delivery the broker dispatched before the cancel can still arrive. A nowait cancel has no reply to retire it, so `_nowait_cancelled` bounds how many of those tags `_cancelled` retains; see the `basic_cancel` docstring for the eviction semantics.

Every end of a consumer entry goes through `Channel._cleanup_consumer_ref`, which pops the tag from both structures. Three paths reach it, each for a different reason: `_on_cancelok` after a client cancel's `Basic.CancelOk`, `_on_cancel` for a broker-initiated cancel with no client cancel in flight, and `_retain_cancelled_tag` when a nowait tag is evicted past the cap. `_cleanup` is the separate channel-wide teardown on the transition to CLOSED: it drops every entry at once, including retained nowait ones, rather than retiring consumers one at a time.

`BlockingChannel` mirrors this one layer up in `_consumer_infos`, which maps a tag to a `_ConsumerInfo`. `_retire_consumer_info` is its single end-of-life transition, used by the broker-cancel event dispatch, by `basic_cancel`'s completion and by a failed `basic_consume` rollback. Incoming delivery and broker-cancel events resolve their owner through `_consumer_for_event` instead of indexing the map directly, so an event whose consumer was already retired during teardown is dropped rather than raising `KeyError` or reaching a consumer that reused the tag.

## Why a reused tag cannot get the old consumer's queued deliveries

The wire frames for a reused tag carry no generation marker: a `Basic.Deliver` names only the consumer tag and a delivery tag. The core `Channel` therefore cannot tell a straggling delivery for a retired consumer from a delivery for the consumer that reused the tag once the old entry is gone. Its protection is ordering, not filtering. A waited cancel retires the entry only when `Basic.CancelOk` arrives, and `basic_consume` rejects a tag still in `_cancelled`, so the tag cannot be reused before the old consumer is fully retired. The residual case is stated in `basic_cancel`'s docstring: once a nowait-cancelled tag is evicted past the retention cap, a delivery the broker had already dispatched for it is indistinguishable and is logged as unexpected.

The queued-delivery case the wrapper has to solve is different. `BlockingChannel` does not run delivery callbacks inline; `_on_consumer_message_delivery` appends a `_ConsumerDeliveryEvt` to `_pending_events` and the user-facing thread drains it in `_dispatch_events`. The guarantee is that `basic_cancel` returns only after `_remove_pending_deliveries` has drained every event queued for the tag. A manual-ack consumer's drained deliveries are rejected with `basic_reject`, one frame per delivery tag on the same channel; an auto-ack consumer's are returned to the caller, since the broker dropped its copy when it dispatched them and there is nothing to reject. Only after that drain can the same tag be consumed again, so an event already queued for the old lifetime is never present when the new consumer's events dispatch. The tests pin callback identity and the reject frame's channel and delivery tag rather than merely that the containers end empty.

## The two cancel paths stay separate

Client-initiated and broker-initiated cancellation are different transitions and the code keeps them apart end to end.

A client cancel sends `Basic.Cancel`, adds the tag to `_cancelled`, and waits for `Basic.CancelOk` when a completion callback was given; the reply retires the entry in `_on_cancelok`. In the wrapper the `_ConsumerInfo` moves to `TEARING_DOWN`, queued deliveries are rejected or returned, and the entry is retired in `basic_cancel`'s `finally`.

A broker cancel arrives as an unsolicited `Basic.Cancel` and is handled by `Channel._on_cancel`, which answers with `Basic.CancelOk` only when the broker advertises the capability and then retires the entry directly, unless a client cancel is already in flight, in which case the client's own `CancelOk` still owns the retirement. The wrapper marks the info `CANCELLED_BY_BROKER`, queues a `_ConsumerCancellationEvt`, and never sends a client `Basic.Cancel` for it; the event's dispatch retires the entry. A broker cancel racing a client cancel must not overwrite `TEARING_DOWN`, because the wrapper is still waiting on its own reply.

## Closing drains before the channel goes away

`Channel.close` calls `_cancel_all_consumers`, the named form of the loop that issues a nowait cancel for every consumer not already cancelling, before the channel leaves OPEN. A delivery the broker keeps dispatching after that point is discarded in `_on_deliver` because a callback could not acknowledge it on a closing channel; the transition to CLOSED reports the count once. Callers that need the backlog run first cancel each consumer with a callback and close from the last `Basic.CancelOk`, which is what `BlockingChannel.close` and `pika.Connection` do. The measured numbers and the rejected alternatives for that ordering live in `thread-safe-connection.md`; this page only states the lifecycle they rely on.
