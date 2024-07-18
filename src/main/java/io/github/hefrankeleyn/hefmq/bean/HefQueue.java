package io.github.hefrankeleyn.hefmq.bean;

import java.util.Iterator;
import java.util.Objects;

/**
 * @Date 2024/7/18
 * @Author lifei
 */
public class HefQueue<Item> implements Iterable<Item>{

    private Node first;
    private Node last;
    private int N;

    private class Node {
        private Item item;
        private Node next;
    }

    public void enqueue(Item item) {
        Node oldLast = last;
        last = new Node();
        last.item = item;
        last.next = null;
        if (isEmpty()) {
            first = last;
        } else {
            oldLast.next = last;
        }
        N++;
    }

    public Item dequeue() {
        if (isEmpty()) {
            return null;
        }
        Item item = first.item;
        first = first.next;
        N--;
        if (isEmpty()) {
            last = null;
        }
        return item;
    }

    public int size() {
        return N;
    }

    public boolean isEmpty() {
        return N==0;
    }

    @Override
    public Iterator<Item> iterator() {
        return new HefIterator();
    }

    private class HefIterator implements Iterator<Item> {

        private Node current = first;

        @Override
        public boolean hasNext() {
            return current!=null;
        }

        @Override
        public Item next() {
            Item item = current.item;
            current = current.next;
            return item;
        }
    }
}
