package day4

import TestBase
import org.jetbrains.kotlinx.lincheck.annotations.*
import org.jetbrains.kotlinx.lincheck.paramgen.*

@Param(name = "index", gen = IntGen::class, conf = "0:${ARRAY_SIZE - 1}")
@Param(name = "value", gen = IntGen::class, conf = "0:2")
class AtomicArrayWithDCSSTest : TestBase(IntAtomicArraySequential::class) {
    private val array = AtomicArrayWithDCSS(ARRAY_SIZE, 0)

    @Operation(params = ["index"])
    fun get(index: Int) = array.get(index)

    @Operation(params = ["index", "value", "value"])
    fun cas(index: Int, expected: Int, update: Int) = array.cas(index, expected, update)

    @Operation(
        params = ["index", "value", "value", "index", "value"],
        handleExceptionsAsResult = [IllegalArgumentException::class]
    )
    fun dcss(
        index1: Int, expected1: Int, update1: Int,
        index2: Int, expected2: Int
    ) = array.dcss(index1, expected1, update1, index2, expected2)
}

@Param(name = "index", gen = IntGen::class, conf = "0:${ARRAY_SIZE - 1}")
@Param(name = "value", gen = IntGen::class, conf = "0:2")
class AtomicArrayWithCAS2Test : TestBase(IntAtomicArraySequential::class) {
    private val array = AtomicArrayWithCAS2(ARRAY_SIZE, 0)

    @Operation(params = ["index"])
    fun get(index: Int) = array.get(index)

    @Operation(params = ["index", "value", "value"])
    fun cas(index: Int, expected: Int, update: Int) = array.cas(index, expected, update)

    @Operation(
        params = ["index", "value", "value", "index", "value", "value"],
        handleExceptionsAsResult = [IllegalArgumentException::class]
    )
    fun cas2(
        index1: Int, expected1: Int, update1: Int,
        index2: Int, expected2: Int, update2: Int
    ) = array.cas2(index1, expected1, update1, index2, expected2, update2)
}

class IntAtomicArraySequential {
    private val array = IntArray(ARRAY_SIZE)

    fun get(index: Int): Int = array[index]

    fun set(index: Int, value: Int) {
        array[index] = value
    }

    fun cas(index: Int, expected: Int, update: Int): Boolean {
        if (array[index] != expected) return false
        array[index] = update
        return true
    }

    fun dcss(
        index1: Int, expected1: Int, update1: Int,
        index2: Int, expected2: Int
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }
        if (array[index1] != expected1 || array[index2] != expected2) return false
        array[index1] = update1
        return true
    }

    fun cas2(
        index1: Int, expected1: Int, update1: Int, index2: Int, expected2: Int, update2: Int
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }
        if (array[index1] != expected1 || array[index2] != expected2) return false
        array[index1] = update1
        array[index2] = update2
        return true
    }
}

private const val ARRAY_SIZE = 3