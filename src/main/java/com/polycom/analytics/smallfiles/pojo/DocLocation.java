package com.polycom.analytics.smallfiles.pojo;

public class DocLocation
{
    private String index;
    private String type;
    private String id;

    @Override
    public String toString()
    {
        return "DocLocation [index=" + index + ", type=" + type + ", id=" + id + "]";
    }

    public String getIndex()
    {
        return index;
    }

    public void setIndex(String index)
    {
        this.index = index;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

}
