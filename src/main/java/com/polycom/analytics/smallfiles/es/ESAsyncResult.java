package com.polycom.analytics.smallfiles.es;

public class ESAsyncResult
{
    private boolean isSucc = true;
    private String msg;
    private Throwable throwable;

    @Override
    public String toString()
    {
        return "ESAsyncResult [isSucc=" + isSucc + ", msg=" + msg + ", throwable=" + throwable + "]";
    }

    public boolean isSucc()
    {
        return isSucc;
    }

    public void setSucc(boolean isSucc)
    {
        this.isSucc = isSucc;
    }

    public String getMsg()
    {
        return msg;
    }

    public void setMsg(String msg)
    {
        this.msg = msg;
    }

    public Throwable getThrowable()
    {
        return throwable;
    }

    public void setThrowable(Throwable throwable)
    {
        this.throwable = throwable;
    }

}
