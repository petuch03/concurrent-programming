package day1

import kotlinx.atomicfu.*
import java.util.concurrent.*

class TreiberStackWithElimination<E> : Stack<E> {
    private val stack = TreiberStack<E>()

    // TODO: Try to optimize concurrent push and pop operations, synchronizing them in an `eliminationArray` cell.
    private val eliminationArray = atomicArrayOfNulls<Any?>(ELIMINATION_ARRAY_SIZE)

    override fun push(element: E) {
        if (tryPushElimination(element)) return
        stack.push(element)
    }

    private fun tryPushElimination(element: E): Boolean {
        // TODO: Choose a random cell in `eliminationArray` and try to install the element there. Wait `ELIMINATION_WAIT_CYCLES` loop cycles in hope that a concurrent `pop()` grabs the element. If so, clean the cell and finish, returning `true`. Otherwise, move the cell to the empty state and return `false`.
        val index = randomCellIndex()
        val cell = eliminationArray[index]
        for (i in 0 until ELIMINATION_WAIT_CYCLES) {
            if (cell.compareAndSet(element, CELL_STATE_RETRIEVED)) {
                return true
            }
        }
        cell.compareAndSet(element, CELL_STATE_EMPTY)
        return false
    }

    override fun pop(): E? = tryPopElimination() ?: stack.pop()

    private fun tryPopElimination(): E? {
        // TODO: Choose a random cell in `eliminationArray` and try to retrieve an element from there. On success, return the element. Otherwise, if the cell is empty, return `null`.
        val index = randomCellIndex()
        val cell = eliminationArray[index]
        val value = cell.value
        if (value == CELL_STATE_RETRIEVED) {
            @Suppress("UNCHECKED_CAST")
            return value as E
        }
        return null
    }

    private fun randomCellIndex(): Int =
        ThreadLocalRandom.current().nextInt(eliminationArray.size)

    companion object {
        private const val ELIMINATION_ARRAY_SIZE = 2 // Do not change!
        private const val ELIMINATION_WAIT_CYCLES = 1 // Do not change!

        // Initially, all cells are in EMPTY state.
        private val CELL_STATE_EMPTY = null

        // `tryPopElimination()` moves the cell state
        // to `RETRIEVED` if the cell contains element.
        private val CELL_STATE_RETRIEVED = Any()
    }
}