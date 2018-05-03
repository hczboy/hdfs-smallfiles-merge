package com.polycom.analytics.smallfiles.pojo;

public class ArchivedPathDocLocation extends DocLocation
{
    private String archivedPath;

    private String fileID;

    public String getFileID()
    {
        return fileID;
    }

    public void setFileID(String fileID)
    {
        this.fileID = fileID;
    }

    public String getArchivedPath()
    {
        return archivedPath;
    }

    public void setArchivedPath(String archivedPath)
    {
        this.archivedPath = archivedPath;
    }

    @Override
    public String toString()
    {
        return "ArchivedPathDocLocation [archivedPath=" + archivedPath + ", fileID=" + fileID + ", toString()="
                + super.toString() + "]";
    }

}
