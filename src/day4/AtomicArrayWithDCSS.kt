@file:Suppress("UNCHECKED_CAST")

package day4

import kotlinx.atomicfu.*

// This implementation never stores `null` values.
class AtomicArrayWithDCSS<E : Any>(size: Int, initialValue: E) {
    private val array = atomicArrayOfNulls<Any?>(size)

    init {
        // Fill array with the initial value.
        for (i in 0 until size) {
            array[i].value = initialValue
        }
    }

    fun get(index: Int): E? {
        val curValue = array[index].value
        return if (curValue is AtomicArrayWithDCSS<*>.DCSSDescriptor) {
            curValue.applyOperation()
            if (curValue.status.value == Status.SUCCESS) {
                curValue.update1
            }
            else {
                curValue.expected1
            }
        } else {
            curValue
        } as E?
    }

    fun cas(index: Int, expected: E?, update: E?): Boolean {
        // TODO: the cell can store a descriptor
        while (true) {
            val curValue = array[index].value
            return if (curValue is AtomicArrayWithDCSS<*>.DCSSDescriptor) {
                curValue.applyOperation()
                continue
            } else if (curValue == expected) {
                if (!array[index].compareAndSet(expected, update)) {
                    continue
                }
                true
            } else {
                false
            }
        }
    }

    fun dcss(
        index1: Int, expected1: E?, update1: E?,
        index2: Int, expected2: E?
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }
        // TODO This implementation is not linearizable!
        // TODO Store a DCSS descriptor in array[index1].
        val descriptor = DCSSDescriptor(index1, expected1, update1, index2, expected2)
        while (true) {
            val index1Value = array[index1].value
            if (index1Value == expected1) {
                if (array[index1].compareAndSet(expected1, descriptor)) {
                    descriptor.applyOperation()
                    return descriptor.status.value === Status.SUCCESS
                }
                continue
            } else if (index1Value is AtomicArrayWithDCSS<*>.DCSSDescriptor) {
                index1Value.applyOperation()
                continue
            } else {
                return false
            }
        }
    }

    private inner class DCSSDescriptor(
        val index1: Int, val expected1: E?, val update1: E?,
        val index2: Int, val expected2: E?
    ) {
        val status = atomic(Status.UNDECIDED)

        fun applyOperation() {
            val tmp = array[index2].value
            val currentValue: E? = if (tmp is AtomicArrayWithDCSS<*>.DCSSDescriptor) {
                if (tmp.status.value === Status.SUCCESS)
                    tmp.update1 as E?
                else
                    tmp.expected1 as E?
            }  else {
                tmp as E?
            }

            if (currentValue == expected2) {
                status.compareAndSet(Status.UNDECIDED, Status.SUCCESS)
            } else {
                status.compareAndSet(Status.UNDECIDED, Status.FAILED)
            }

            when (status.value) {
                Status.SUCCESS -> {
                    array[index1].compareAndSet(this, update1)
                }
                Status.FAILED -> {
                    array[index1].compareAndSet(this, expected1)
                }
                else -> {}
            }
        }
    }

    private enum class Status {
        UNDECIDED, FAILED, SUCCESS
    }
}