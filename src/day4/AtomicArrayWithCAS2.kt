@file:Suppress("DuplicatedCode", "UNCHECKED_CAST")

package day4

import kotlinx.atomicfu.*
import kotlin.math.min

// This implementation never stores `null` values.
class AtomicArrayWithCAS2<E : Any>(size: Int, initialValue: E) {
    private val array = atomicArrayOfNulls<Any>(size)

    init {
        // Fill array with the initial value.
        for (i in 0 until size) {
            array[i].value = initialValue
        }
    }

    fun get(index: Int): E? {
        // TODO: the cell can store a descriptor
        while (true) {
            val currentValue = array[index].value
            return when (currentValue) {
                is AtomicArrayWithCAS2<*>.CAS2Descriptor -> {
                    if (currentValue.status.value == Status.SUCCESS) {
                        if (index == currentValue.index1) currentValue.update1
                        else currentValue.update2
                    } else {
                        if (index == currentValue.index1) currentValue.expected1
                        else currentValue.expected2
                    }
                }

                is AtomicArrayWithCAS2<*>.DCSSDescriptor -> {
                    currentValue.applyOperation()
                    val resultOfDCSS = if (currentValue.status.value == Status.SUCCESS) currentValue.update1
                    else currentValue.expected1
                    if (resultOfDCSS is AtomicArrayWithCAS2<*>.CAS2Descriptor) continue
                    resultOfDCSS
                }

                else -> {
                    currentValue
                }
            } as E?
        }
    }

    fun cas(index: Int, expected: E?, update: E?): Boolean {
        // TODO: the cell can store a descriptor
        while (true) {
            val currentValue = array[index].value
            if (currentValue is AtomicArrayWithCAS2<*>.CAS2Descriptor) {
                currentValue.applyOperation()
                continue
            } else if (currentValue is AtomicArrayWithCAS2<*>.DCSSDescriptor) {
                currentValue.applyOperation()
                continue
            } else if (currentValue == expected) {
                if (array[index].compareAndSet(expected, update)) {
                    return true
                }
                continue
            } else {
                return false
            }
        }
    }

    fun cas2(
        index1: Int, expected1: E?, update1: E?,
        index2: Int, expected2: E?, update2: E?
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }
        // TODO: this implementation is not linearizable,
        // TODO: Store a CAS2 descriptor in array[index1].
        while (true) {
            val firstValue = array[index1].value
            val secondValue = array[index2].value

            if (firstValue is AtomicArrayWithCAS2<*>.CAS2Descriptor) {
                firstValue.applyOperation()
                continue
            }
            if (firstValue is AtomicArrayWithCAS2<*>.DCSSDescriptor) {
                firstValue.applyOperation()
                continue
            }
            if (secondValue is AtomicArrayWithCAS2<*>.CAS2Descriptor) {
                secondValue.applyOperation()
                continue
            }
            if (secondValue is AtomicArrayWithCAS2<*>.DCSSDescriptor) {
                secondValue.applyOperation()
                continue
            }

            if (firstValue != expected1 || secondValue != expected2) {
                return false
            }

            val descriptorToUse = if (index1 < index2) {
                CAS2Descriptor(index1, expected1, update1, index2, expected2, update2)
            } else {
                CAS2Descriptor(index2, expected2, update2, index1, expected1, update1)
            }
            val minIndexValue = if (index1 < index2) expected1 else expected2
            if (array[min(index1, index2)].compareAndSet(minIndexValue, descriptorToUse)) {
                descriptorToUse.applyOperation()
                return descriptorToUse.status.value == Status.SUCCESS
            }
        }
    }

    private inner class CAS2Descriptor(
        val index1: Int, val expected1: E?, val update1: E?,
        val index2: Int, val expected2: E?, val update2: E?
    ) {
        val status = atomic(Status.UNDECIDED)

        fun applyOperation() {
            while (true) {
                if (dcss(index2, expected2, this, this)) {
                    status.compareAndSet(Status.UNDECIDED, Status.SUCCESS)
                } else {
                    val secondValue = array[index2].value
                    if (secondValue == this) {
                        status.compareAndSet(Status.UNDECIDED, Status.SUCCESS)
                    } else if (secondValue is AtomicArrayWithCAS2<*>.CAS2Descriptor) {
                        secondValue.applyOperation()
                        continue
                    } else if (secondValue is AtomicArrayWithCAS2<*>.DCSSDescriptor) {
                        secondValue.applyOperation()
                        continue
                    } else {
                        status.compareAndSet(Status.UNDECIDED, Status.FAILED)
                    }
                }

                when (status.value) {
                    Status.UNDECIDED -> {
                        status.compareAndSet(Status.UNDECIDED, Status.FAILED)
                        continue
                    }

                    Status.FAILED -> break
                    else -> {}
                }

                array[index1].compareAndSet(this, update1)
                if (array[index1].value == this) {
                    continue
                }

                array[index2].compareAndSet(this, update2)
                if (array[index2].value == this) {
                    continue
                }
                break
            }

            if (status.value == Status.FAILED) {
                array[index2].compareAndSet(this, expected2)
                array[index1].compareAndSet(this, expected1)
            }
        }
    }

    private fun dcss(
        index1: Int, expected1: Any?, update1: Any?,
        caS2Descriptor: CAS2Descriptor
    ): Boolean {
        // TODO This implementation is not linearizable!
        // TODO Store a DCSS descriptor in array[index1].

        while (true) {
            val currentValue = array[index1].value
            if (currentValue == caS2Descriptor) {
                return true
            } else if (currentValue is AtomicArrayWithCAS2<*>.DCSSDescriptor) {
                currentValue.applyOperation()
                continue
            } else if (currentValue is AtomicArrayWithCAS2<*>.CAS2Descriptor) {
                currentValue.applyOperation()
                continue
            } else {
                val descriptorToUse = DCSSDescriptor(index1, expected1, update1, caS2Descriptor)
                if (array[index1].compareAndSet(expected1, descriptorToUse)) {
                    descriptorToUse.applyOperation()
                    return descriptorToUse.status.value == Status.SUCCESS
                }
                val newValue = array[index1].value
                if (newValue is AtomicArrayWithCAS2<*>.CAS2Descriptor ||
                    newValue is AtomicArrayWithCAS2<*>.DCSSDescriptor || newValue == expected1
                ) {
                    continue
                } else {
                    return false
                }
            }
        }
    }

    private inner class DCSSDescriptor(
        val index1: Int, val expected1: Any?, val update1: Any?,
        val caS2Descriptor: CAS2Descriptor
    ) {
        val status = atomic(Status.UNDECIDED)
        fun applyOperation() {
            while (true) {
                val resultValue = array[index1].value
                if (resultValue == this) {
                    if (caS2Descriptor.status.value == Status.UNDECIDED) {
                        status.compareAndSet(Status.UNDECIDED, Status.SUCCESS)
                    } else {
                        status.compareAndSet(Status.UNDECIDED, Status.FAILED)
                    }

                    when (status.value) {
                        Status.UNDECIDED -> {
                            status.compareAndSet(Status.UNDECIDED, Status.FAILED)
                            continue
                        }

                        Status.FAILED -> {
                            break
                        }

                        else -> {}
                    }
                    array[index1].compareAndSet(this, update1)
                    if (array[index1].value == this) {
                        continue
                    }
                    break
                } else if (resultValue is AtomicArrayWithCAS2<*>.DCSSDescriptor) {
                    resultValue.applyOperation()
                    continue
                } else if (resultValue is AtomicArrayWithCAS2<*>.CAS2Descriptor) {
                    resultValue.applyOperation()
                    continue
                } else {
                    status.compareAndSet(Status.UNDECIDED, Status.FAILED)
                    break
                }
            }

            if (status.value == Status.FAILED) {
                array[index1].compareAndSet(this, expected1)
            }
        }
    }

    private enum class Status {
        UNDECIDED, FAILED, SUCCESS
    }
}