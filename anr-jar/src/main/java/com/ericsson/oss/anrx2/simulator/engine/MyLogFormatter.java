package com.ericsson.oss.anrx2.simulator.engine;

import java.io.*;
import java.text.*;
import java.util.Date;
import java.util.logging.*;

public class MyLogFormatter extends Formatter 
{
    private static SimpleDateFormat m_DF = new SimpleDateFormat("dd-HH:mm:ss.SSS");
    
    public String format(LogRecord record) 
    {
        StringBuffer sb = new StringBuffer();


        sb.append(m_DF.format(new Date(record.getMillis())));
        sb.append(" ");
        sb.append(record.getLevel());
        sb.append(" ");
        sb.append(record.getThreadID());
        sb.append(" ");
        
        sb.append(record.getLoggerName());
        sb.append(" ");

        String message = formatMessage(record);
        sb.append(message);
        sb.append("\n");
        
        if (record.getThrown() != null) {
            try {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                record.getThrown().printStackTrace(pw);
                pw.close();
                sb.append(sw.toString());
            } catch (Exception ex) {
            }
        }
        return sb.toString();
    }
}
