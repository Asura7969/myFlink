package com.myFlink.java.project.link.bean;

import java.util.TreeMap;

public class Link {

    public TreeMap<String,Node> link;
    public long windowEnd;

    public Link() {
    }

    public Link(long windowEnd) {
        this.windowEnd = windowEnd;
        link = new TreeMap<>();
    }

    public void addNode(String rpcId, Node node){
        link.put(rpcId,node);
    }

    public TreeMap<String, Node> getLink() {
        return link;
    }

    public void setLink(TreeMap<String, Node> link) {
        this.link = link;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return link.toString();
        //return "{" +
        //        "link=" + link.toString() +
        //        ", windowEnd=" + windowEnd +
        //        '}';
    }
}
