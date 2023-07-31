package day2

import day1.*
import kotlinx.atomicfu.*

class FAABasedQueueSimplified<E> : Queue<E> {
    private val infiniteArray = atomicArrayOfNulls<Any?>(1024) // conceptually infinite array
    private val enqIdx = atomic(0)
    private val deqIdx = atomic(0)

    override fun enqueue(element: E) {
        // TODO: Increment the counter atomically via Fetch-and-Add. Use `getAndIncrement()` function for that.
        // TODO: Atomically install the element into the cell if the cell is not poisoned.
        while (true) {
            val i = enqIdx.getAndIncrement()
            if (infiniteArray[i].compareAndSet(null, element)) {
                return
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        // TODO: Increment the counter atomically via Fetch-and-Add. Use `getAndIncrement()` function for that.
        // TODO: Try to retrieve an element if the cell contains an element, poisoning the cell if it is empty.

        while (true) {
            val e1 = enqIdx.value
            val d1 = deqIdx.value
            val e2 = enqIdx.value
            if (e1 == e2) {
                if (e1 <= d1) return null
            } else {
                continue
            }
            /*
            enqIdx монотонно растет, а deqIdx и так мог убежать вперед. Поэтому если у нас enqIdx
            остался такой же, то у нас верный снэпшот и мы можем идти дальше. Иначе -- неверный, надо перечитывать
            значения заново
            */
            val i = deqIdx.getAndIncrement()
            if (infiniteArray[i].compareAndSet(null, POISONED)) {
                continue
            } else {
                return infiniteArray[i].value as E?
            }
        }
    }
}

// TODO: poison cells with this value.
private val POISONED = Any()
