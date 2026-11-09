package com.skybook.inventory.holds;

/** INVENTORY_LOCK_STRATEGY: pessimistic = SELECT ... FOR UPDATE, optimistic = versioned UPDATE with retries. */
public enum LockStrategy {
    PESSIMISTIC,
    OPTIMISTIC
}
