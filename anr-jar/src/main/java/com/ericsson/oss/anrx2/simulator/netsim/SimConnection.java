package com.ericsson.oss.anrx2.simulator.netsim;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ericsson.oss.anrx2.simulator.engine.Config;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class SimConnection {
	private static Map<String,String> simNameCache = new HashMap<String,String>();
	
	private Session simHostSession;
	private ChannelExec netsimPipeChannel;
	private ChannelSftp sftpChannel;
	
	private PrintWriter toNP;
	private BufferedReader fromNP;	
    
	private final static Logger logger = Logger.getLogger(SimConnection.class.getName()); 
	
	private final String simName;
	
	public SimConnection(String simHost,String simName) throws Exception {
		logger.finer("<init> simHost=" + simHost + " simName=" + simName);
		this.simName = simName;
		
		String password = Config.getInstance().getProps().getProperty("simhost." + simHost + ".password");
		if ( password == null ) {
			password = "netsim";
		}
		
		JSch jsch=new JSch();
		try {
		simHostSession=jsch.getSession("netsim", simHost);
		simHostSession.setPassword(password);
		simHostSession.setConfig("StrictHostKeyChecking", "no");
		logger.fine("Opening connection to " + simHostSession.getHost());
		simHostSession.connect(30000);
		
		netsimPipeChannel = (ChannelExec)simHostSession.openChannel("exec");
		netsimPipeChannel.setCommand("/netsim/inst/netsim_pipe");		
		netsimPipeChannel.connect(120000);		

		toNP = new PrintWriter(netsimPipeChannel.getOutputStream());
		fromNP = new BufferedReader(new InputStreamReader(netsimPipeChannel.getInputStream()));
		
	    sftpChannel = (ChannelSftp)simHostSession.openChannel("sftp");
	    sftpChannel .connect();
		sftpChannel.cd("/tmp");
		} catch ( JSchException jschExcept ) {
			logger.log(Level.SEVERE, "Failed to connect to " + simHost, jschExcept);
			throw jschExcept;
		}
		
		openSim();
	}

	public List<String> getChildren(String meId, String baseLdn) throws Exception {
		if ( ! isReplyOkay(sendCommand(".select " + meId)) ) {
			throw new Exception("Failed to select node " + meId + " in " + this.simName);
		}
		
		return null;			
	}
	
	public void execKertayle(String srcMeId, KertayleSession kSess) throws Exception {
		execKertayle(srcMeId,kSess,true);
	}

	public void execKertayle(String srcMeId, KertayleSession kSess, boolean commitScript) throws Exception {
		boolean hasContent = kSess.hasContent();
		logger.fine("execKertayle " + simHostSession.getHost() + " simName=" + this.simName + 
				" srcMeId=" + srcMeId + ", hasContent=" + hasContent);
		
		// If the session has nothing in it, then jump out early
		if ( ! hasContent ) {
			return;
		}
		
		// Hack for ENM cause they're putting the netsim host in the MeContext Id
		if ( srcMeId.indexOf("_") != -1 ) {
			srcMeId = srcMeId.substring(srcMeId.indexOf("_") + 1);
		}
		
		if ( ! isReplyOkay(sendCommand(".select " + srcMeId)) ) {
			throw new Exception("Failed to select node " + srcMeId + " in " + this.simName);
		}			
		
		File kerFile = File.createTempFile(srcMeId +"_",".ker");
		PrintWriter kerOut = new PrintWriter(kerFile);
		kSess.dump(kerOut);
		kerOut.close();
		logger.fine(simHostSession.getHost() + " wrote to " + kerFile.getAbsolutePath());

		String remotePath = "/tmp/" + srcMeId + ".ker";		
		sftpChannel.put(kerFile.getAbsolutePath(),remotePath);
		logger.fine(simHostSession.getHost() + " sent to " + remotePath);
				
		String commitType = "script";
		if ( ! commitScript ) {
			commitType = "operation";
		}
		if ( ! isReplyOkay(sendCommand("kertayle:file=\"" + remotePath + "\",commit_freq=" + commitType + ";")) ) {
			throw new Exception("Failed to exec kertayle file " + kerFile.getAbsolutePath());
		}
		
		kerFile.delete();
		sftpChannel.rm(remotePath);
	}
	

	public void close() {
		logger.fine("Closing connection to " + simHostSession.getHost());
		netsimPipeChannel.disconnect();
		sftpChannel.disconnect();
		simHostSession.disconnect();
	}

	public String getHost() {
		return simHostSession.getHost();
	}

	public String getSimName() {
		return simName;
	}
	
	private void openSim() throws Exception {		
		String fullSimName = null;
		logger.finer("openSim simName=" + fullSimName);
		synchronized (simNameCache) {
			fullSimName = simNameCache.get(simName);
			if ( fullSimName == null ) {
				List<String> reply = sendCommand(".show started");
				String firstERBSNode = simName + "ERBS00001";
				String firstDG2Node = simName + "dg2ERBS00001";
				for ( Iterator<String> replyItr = reply.iterator(); fullSimName == null && replyItr.hasNext(); ) {
					String line = replyItr.next();
					if ( line.contains(firstERBSNode) || (line.contains(firstDG2Node))) {
						String lineParts[] = line.split(" ");
						String pathParts[] = lineParts[lineParts.length - 1].split("/");						
						fullSimName = pathParts[pathParts.length-1];
						simNameCache.put(simName,fullSimName);
					}
				}
			}
		}
		logger.finer("openSim fullSimName=" + fullSimName);

		if ( fullSimName == null ) {
			throw new Exception("Cannot find simulation for " + simName);
		}
		
		if ( ! isReplyOkay(sendCommand(".open " + fullSimName)) ) {
			throw new Exception("Failed to open simulation " + fullSimName);
		}
	}
	
	private List<String> sendCommand(String command) throws Exception {
		toNP.println(command + "\r");
		logger.fine(simHostSession.getHost() + " >" + command);
		toNP.println(".echo EOC\r");
		toNP.flush();
		
		List<String> result = new LinkedList<String>();
		boolean foundEOC = false;
		while ( ! foundEOC ) {
			String line = fromNP.readLine();
			logger.fine(simHostSession.getHost() + " <" + line);
			if ( line.startsWith("EOC") ) {
				foundEOC = true;
			} else {
				result.add(line);
			}
		}
		return result;
	}
	
	private boolean isReplyOkay(List<String> reply) {
		boolean foundOkay = false;
		for ( Iterator<String> replyItr = reply.iterator(); foundOkay == false && replyItr.hasNext(); ) {
			String line = replyItr.next();
			if ( line.equals("OK") ) {
				foundOkay = true;
			}
		}
		return foundOkay;
	}

}
