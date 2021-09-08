package com.yasserm.negmasgeniusbridge;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.logging.*;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.lang.Class;
import java.nio.charset.Charset;

import genius.core.*;
import genius.core.protocol.Protocol;
import genius.core.repository.AgentRepItem;
import genius.core.repository.DomainRepItem;
import genius.core.repository.ProfileRepItem;
import genius.core.repository.ProtocolRepItem;
import genius.core.tournament.VariablesAndValues.AgentParamValue;
import genius.core.tournament.VariablesAndValues.AgentParameterVariable;
import py4j.GatewayServer;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
//import genius.core.exceptions.NegotiationPartyTimeoutException;
import genius.core.session.ExecutorWithTimeout;
import genius.core.actions.Accept;
import genius.core.actions.Inform;
import genius.core.actions.Action;
import genius.core.actions.EndNegotiation;
import genius.core.actions.Offer;
import genius.core.issue.Issue;
import genius.core.issue.IssueDiscrete;
import genius.core.issue.Value;
import genius.core.issue.ValueDiscrete;
import genius.core.parties.AbstractNegotiationParty;
import genius.core.parties.NegotiationInfo;
import genius.core.persistent.DefaultPersistentDataContainer;
import genius.core.persistent.PersistentDataType;
import genius.core.timeline.ContinuousTimeline;
import genius.core.timeline.DiscreteTimeline;
import genius.core.timeline.TimeLineInfo;
import genius.core.utility.AbstractUtilitySpace;
import genius.core.utility.AdditiveUtilitySpace;
import genius.core.protocol.StackedAlternatingOffersProtocol;
import py4j.Py4JNetworkException;

import java.util.concurrent.ThreadLocalRandom;

import java.util.logging.Logger;

class MapUtil {
	public static String mapToString(Map<String, String> map, String entry_separator, String internal_separator) {
		StringBuilder stringBuilder = new StringBuilder();

		for (String key : map.keySet()) {
			if (stringBuilder.length() > 0) {
				stringBuilder.append(entry_separator);
			}
			String value = map.get(key);
			// try {
			// stringBuilder.append((key != null ? URLEncoder.encode(key, "UTF-8") : ""));
			stringBuilder.append((key != null ? key : ""));
			stringBuilder.append(internal_separator);
			stringBuilder.append(value != null ? value : "");
			// stringBuilder.append(value != null ? URLEncoder.encode(value, "UTF-8") : "");
			// } catch (UnsupportedEncodingException e) {
			// throw new RuntimeException("This method requires UTF-8 encoding support", e);
			// }
		}

		return stringBuilder.toString();
	}

	public static Map<String, String> stringToMap(String input, String entry_separator, String internal_separator) {
		Map<String, String> map = new HashMap<String, String>();

		String[] nameValuePairs = input.split(entry_separator);
		for (String nameValuePair : nameValuePairs) {
			String[] nameValue = nameValuePair.split(internal_separator);
			// try {
			// map.put(URLDecoder.decode(nameValue[0], "UTF-8"), nameValue.length > 1 ?
			// URLDecoder.decode(
			// nameValue[1], "UTF-8") : "");
			map.put(nameValue[0], nameValue.length > 1 ? nameValue[1] : "");
			// } catch (UnsupportedEncodingException e) {
			// throw new RuntimeException("This method requires UTF-8 encoding support", e);
			// }
		}

		return map;
	}
}

class NegLoader {
	private long n_total_agents = 0;
	private long n_total_negotiations = 0;
	private long n_active_negotiations = 0;
	private long n_active_agents = 0;
	private long n_total_offers = 0;
	private long n_total_responses = 0;
	public String jarName = "genius-8.0.4-jar-with-dependencies.jar";
	private HashMap<String, AbstractNegotiationParty> parties = null;
	private HashMap<String, AgentAdapter> agents = null;
	private HashMap<String, Boolean> is_party = null;
	private HashMap<String, NegotiationInfo> infos = null;
	private HashMap<String, AgentID> ids = null;
	private HashMap<String, Domain> domains = null;
	private HashMap<String, AdditiveUtilitySpace> util_spaces = null;
	private HashMap<String, Boolean> first_actions = null;
	private HashMap<String, ExecutorWithTimeout> executors = null;
	private HashMap<String, ArrayList<Issue>> issues_all;
	private HashMap<String, HashMap<String, HashMap<String, Value>>> string2values = null;
	private HashMap<String, HashMap<String, Issue>> string2issues = null;
	private HashMap<String, TimeLineInfo> timelines = null;
	private GatewayServer server = null;
	private int n_agents = 0;

	private String INTERNAL_SEP = "<<s=s>>";
	private String ENTRY_SEP = "<<y,y>>";
	private String FIELD_SEP = "<<sy>>";

	private long global_timeout = 180;
	private boolean force_timeout = true;
	private boolean force_any_timeout = true;
	private boolean force_timeout_init = false;
	private boolean force_timeout_end = false;
	private Logger logger = null;
	private boolean logging = true;
	public boolean is_debug = false;
	private boolean is_silent = true;

	public class Serialize implements Serializable {
	}

	public NegLoader(){
	    this(false, false, false, false, 0, true, null, true);
	}
	public NegLoader(boolean is_debug, boolean force_timeout, boolean force_timeout_init, boolean force_timeout_end,
			long timeout, boolean logging, Logger logger,  boolean is_silent) {
		this.logger = logger;
		this.logging = logging;
		this.is_debug = is_debug;
		this.is_silent = is_silent;
		this.force_timeout_init = force_timeout_init;
		this.force_timeout_end = force_timeout_end;
		if (timeout > 0)
			global_timeout = timeout;
		this.force_timeout = force_timeout;
		this.force_any_timeout = force_timeout || force_timeout_end || force_timeout_init;
		parties = new HashMap<String, AbstractNegotiationParty>();
		agents = new HashMap<String, AgentAdapter>();
		is_party = new HashMap<String, Boolean>();
		infos = new HashMap<String, NegotiationInfo>();
		ids = new HashMap<String, AgentID>();
		domains = new HashMap<String, Domain>();
		util_spaces = new HashMap<String, AdditiveUtilitySpace>();
		first_actions = new HashMap<String, Boolean>();
		if (force_any_timeout)
			executors = new HashMap<String, ExecutorWithTimeout>();
		string2values = new HashMap<String, HashMap<String, HashMap<String, Value>>>();
		issues_all = new HashMap<String, ArrayList<Issue>>();
		timelines = new HashMap<String, TimeLineInfo>();
		string2issues = new HashMap<String, HashMap<String, Issue>>();
	}

	/// Python Hooks: Methods called from python (they all have python snake_case
	/// naming convension)
	public String test(String class_name) {
		info(String.format("Test is called with class-name %s", class_name));
		ArrayList classes = new ArrayList();

		if(!is_silent)
			System.out.println("Jar " + jarName);
		try {
			JarInputStream jarFile = new JarInputStream(new FileInputStream(jarName));
			JarEntry jarEntry;

			while (true) {
				jarEntry = jarFile.getNextJarEntry();
				if (jarEntry == null) {
					break;
				}
				if (jarEntry.getName().endsWith(".class")) {
					// System.out.println("Found "
					// + jarEntry.getName().replaceAll("/", "\\."));
					classes.add(jarEntry.getName().replaceAll("/", "\\."));
				}
			}
			Class<?> clazz = Class.forName(class_name);
			if(!is_silent)
				System.out.println(clazz.toString());
			// Constructor<?> constructor = clazz.getConstructor(String.class,
			// Integer.class);
			AbstractNegotiationParty instance = (AbstractNegotiationParty) clazz.newInstance();
			return instance.getDescription();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}

	public String create_agent(String class_name) {
		try {
			// JarInputStream jarFile = new JarInputStream(new FileInputStream(
			// jarName));
			Class<?> clazz = Class.forName(class_name);
			String uuid = class_name + UUID.randomUUID().toString();
			// Constructor<?> constructor = clazz.getConstructor(String.class,
			// Integer.class);
			if (AbstractNegotiationParty.class.isAssignableFrom(clazz)) {
				AbstractNegotiationParty agent = (AbstractNegotiationParty) clazz.newInstance();
				this.is_party.put(uuid, true);
				this.parties.put(uuid, agent);

			} else {
				AgentAdapter agent = (AgentAdapter) clazz.newInstance();
				this.is_party.put(uuid, false);
				this.agents.put(uuid, agent);
			}
			n_total_agents++;
			n_active_agents++;

			if (is_debug) {
				String msg = String.format("Creating Agent of type %s (ID= %s)\n", class_name, uuid);
//				System.out.print(msg);
				info(msg);
//				System.out.flush();
			} else {
				this.printStatus();
			}
			return uuid;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}

	public double get_time(String agent_uuid){
		Boolean isparty = is_party.get(agent_uuid);
		if (isparty == null)
			return -1;
	    if (isparty){
			AbstractNegotiationParty agent = parties.get(agent_uuid);
			TimeLineInfo timeline = agent.getTimeLine();
			if (timeline == null)
				return -2;
			return timeline.getTime();
		}else {
			Agent agent = (Agent) agents.get(agent_uuid);
			if (agent.timeline == null)
				return -3;
			return agent.timeline.getTime();
		}

	}

	public void on_negotiation_start(String agent_uuid, int n_agents, long n_steps, long time_limit, boolean real_time,
			String domain_file_name, String utility_file_name, long agent_timeout) {
		this.n_agents = n_agents;
		Boolean isparty = is_party.get(agent_uuid);
		if (isparty == null) {
			if (is_debug) {
				String msg = String.format("Agent %s does not exist", agent_uuid);
//				System.out.print(msg);
				info(msg);
//				System.out.flush();
			}
			return;
		}
		if (is_debug){
			String msg = String.format("Domain file: %s\nUfun: %s\nAgent: %s", domain_file_name, utility_file_name, agent_uuid);
			info(msg);
//			System.out.println(msg);
		}
		if (isparty) {
			AbstractNegotiationParty agent = this.parties.get(agent_uuid);
			int seed = is_debug ? ThreadLocalRandom.current().nextInt(0, 10000) : 0;
			NegotiationInfo info = createNegotiationInfo(domain_file_name, utility_file_name, real_time,
					real_time ? (int) time_limit : (int) n_steps, seed, agent_uuid, agent_timeout);
			if (info == null)
				return;
			if (force_timeout_init) {
				ExecutorWithTimeout executor = executors.get(agent_uuid);
				try {
					executor.execute(agent_uuid, new Callable<AbstractNegotiationParty>() {
						@Override
						public AbstractNegotiationParty call() throws Exception {
							agent.init(info);
							return agent;

						}
					});
				} catch (TimeoutException e) {
					String msg = "Negotiating party " + agent_uuid + " timed out in init() method.";
					if (logger != null) logger.info(msg);
				} catch (ExecutionException e) {
					String msg = "Negotiating party " + agent_uuid + " threw an exception in init() method.";
					if (logger != null) logger.info(msg);
				}
			} else {
				agent.init(info);
			}
		} else {
			AgentAdapter agent = this.agents.get(agent_uuid);
			NegotiationInfo info = createNegotiationInfo(domain_file_name, utility_file_name, real_time,
					real_time ? (int) time_limit : (int) n_steps, 0, agent_uuid, agent_timeout);
			if (info == null)
				return;
			if (force_timeout_init) {

				ExecutorWithTimeout executor = executors.get(agent_uuid);
				try {
					executor.execute(agent_uuid, new Callable<AgentAdapter>() {
						@Override
						public AgentAdapter call() throws Exception {
							agent.init(info);
							return agent;

						}
					});
				} catch (TimeoutException e) {
					String msg = "Negotiating party " + agent_uuid + " timed out in init() method.";
					if (logger != null) logger.info(msg);
				} catch (ExecutionException e) {
					String msg = "Negotiating party " + agent_uuid + " threw an exception in init() method.";
					if (logger != null) logger.info(msg);
				}
			} else {
				agent.init(info);
			}
		}
		n_total_negotiations++;
		n_active_negotiations++;
		if (is_debug) {
			String msg = String.format("Agent %s: time limit %d, step limit %d\n", getAgentName(agent_uuid), time_limit,
					n_steps);
//			System.out.print(msg);
			info(msg);
//			System.out.flush();
		} else {
			printStatus();
		}
	}

	public String choose_action(String agent_uuid, int round) {
		n_total_responses++;
		// System.out.format("Entered choose actino");
		Boolean isparty = is_party.get(agent_uuid);
		if (isparty == null) {
			if (is_debug) {
				String msg = String.format("Agent %s does not exist", agent_uuid);
//				System.out.print(msg);
				info(msg);
//				System.out.flush();
			}
			return "";
		}
		if (isparty)
			return chooseActionParty(agent_uuid, round);
		return chooseActionAgent(agent_uuid, round);
	}

	public Boolean receive_message(String agent_uuid, String from_id, String typeOfAction, String bid_str, int round) {
		n_total_offers++;
		boolean result;
		Boolean isparty = is_party.get(agent_uuid);
		if (isparty == null) {
			if (is_debug) {
				String msg = String.format("Agent %s does not exist", agent_uuid);
//				System.out.print(msg);
				info(msg);
//				System.out.flush();
			}
			return false;
		}
		if (isparty)
			result = receiveMessageParty(agent_uuid, from_id, typeOfAction, bid_str, round);
		else
			result = receiveMesasgeAgent(agent_uuid, from_id, typeOfAction, bid_str, round);
		return result;
	}

	public void inform_message(String agent_uuid, int agent_num) {
		if (ids.get(agent_uuid) == null) {
			if (is_debug) {
				String msg = String.format("Agent %s does not exist", agent_uuid);
//				System.out.print(msg);
				info(msg);
//				System.out.flush();
			}
			return;
		}
		Inform inform = new Inform(ids.get(agent_uuid), "NumberOfAgents", agent_num);
		parties.get(agent_uuid).receiveMessage(ids.get(agent_uuid), inform);
	}

	public void inform_message(String agent_uuid) {
		if (ids.get(agent_uuid) == null) {
			if (is_debug) {
				String msg = String.format("Agent %s does not exist", agent_uuid);
//				System.out.print(msg);
				info(msg);
//				System.out.flush();
			}
			return;
		}
		Inform inform = new Inform(ids.get(agent_uuid), "NumberOfAgents", this.n_agents);
		parties.get(agent_uuid).receiveMessage(ids.get(agent_uuid), inform);
	}

	public void on_negotiation_end(String agent_uuid, String bid_str) {
		Bid bid = bid_str == null ? null : strToBid(agent_uuid, bid_str);
		Boolean isparty = is_party.get(agent_uuid);
		if (isparty == null) {
			if (is_debug) {
				String msg = String.format("Agent %s does not exist", agent_uuid);
//				System.out.print(msg);
				info(msg);
//				System.out.flush();
			}
			return;
		}
		if (isparty) {
			AbstractNegotiationParty agent = (AbstractNegotiationParty) parties.get(agent_uuid);
			if (force_timeout_end) {
				ExecutorWithTimeout executor = executors.get(agent_uuid);
				try {
					executor.execute(agent.toString(), new Callable<Map<String, String>>() {
						@Override
						public Map<String, String> call() throws Exception {
							return agent.negotiationEnded(bid);
						}
					});
				} catch (TimeoutException e) {
					String msg = "Negotiating party " + agent_uuid + " timed out in negotiationEnded() method.";
					if (logger != null) logger.info(msg);
				} catch (ExecutionException e) {
					String msg = "Negotiating party " + agent_uuid
							+ " threw an exception in negotiationEnded() method.";
					if (logger != null) logger.info(msg);
				}
			} else {
				agent.negotiationEnded(bid);
				// todo: check to see if the original protocol in genius uses this return value
				// for anything
			}
		} else {
			AgentAdapter agent = (AgentAdapter) agents.get(agent_uuid);
			if (force_timeout_end) {
				ExecutorWithTimeout executor = executors.get(agent_uuid);
				try {
					executor.execute(agent.toString(), new Callable<Map<String, String>>() {
						@Override
						public Map<String, String> call() throws Exception {
							return agent.negotiationEnded(bid);
						}
					});
				} catch (TimeoutException e) {
					String msg = "Negotiating party " + agent_uuid + " timed out in negotiationEnded() method.";
					if (logger != null) logger.info(msg);
				} catch (ExecutionException e) {
					String msg = "Negotiating party " + agent_uuid
							+ " threw an exception in negotiationEnded() method.";
					if (logger != null) logger.info(msg);
				}
			} else {
				agent.negotiationEnded(bid);
			}
		}
	}

	public String destroy_agent(String agent_uuid) {
		this.infos.remove(agent_uuid);
		this.ids.remove(agent_uuid);
		this.util_spaces.remove(agent_uuid);
		this.domains.remove(agent_uuid);
		this.timelines.remove(agent_uuid);
		this.first_actions.remove(agent_uuid);
		if (force_any_timeout)
			this.executors.remove(agent_uuid);
		this.string2values.remove(agent_uuid);
		this.string2issues.remove(agent_uuid);
		this.issues_all.remove(agent_uuid);
		this.parties.remove(agent_uuid);
		this.agents.remove(agent_uuid);
		this.is_party.remove(agent_uuid);
		n_active_agents--;
		if (is_debug) {
			String msg = String.format("Agent %s destroyed\n", agent_uuid);
//			System.out.print(msg);
			info(msg);
//			System.out.flush();
		} else {
			printStatus();
		}
		System.gc();
		return "";
	}

	public void clean() {
		this.infos.clear();
		this.ids.clear();
		this.util_spaces.clear();
		this.domains.clear();
		this.timelines.clear();
		this.first_actions.clear();
		if (force_any_timeout)
			this.executors.clear();
		this.string2values.clear();
		this.string2issues.clear();
		this.issues_all.clear();
		this.parties.clear();
		this.agents.clear();
		this.is_party.clear();
		n_active_agents = this.ids.size();
		System.gc();
		if (is_debug) {
			String msg = String.format("All agents destroyed");
//			System.out.print(msg);
			info(msg);
//			System.out.flush();
		} else {
			printStatus();
		}
	}

	public void shutdown() {
		info(String.format("Shutting down Py4j"));
		this.shutdownPy4jServer();
	}

	public void kill_threads(int wait_time) {
		/**
		 * Tries to interrupt all threads (except the main), waits for the wait_time in
		 * milliseconds then kills them all.
		 */
		info("Kill threads was sent to this bridge");
		Thread current = Thread.currentThread();
		for (Thread t : Thread.getAllStackTraces().keySet()) {
			if (current != t && t.getState() == Thread.State.RUNNABLE)
				t.interrupt();
		}

		try {
			Thread.sleep(wait_time);
		} catch (InterruptedException e) {
			String msg = String.format("Main thread interrupted!!");
//			System.out.print(msg);
			info(msg);
			current.interrupt();
			return;
		}
		for (Thread t : Thread.getAllStackTraces().keySet()) {
			if (current != t && t.getState() == Thread.State.RUNNABLE)
				t.stop();
		}
	}

	public void kill() {
		info("Kill was sent to this bridge");
		this.shutdown();
		System.exit(0);
	}

	/* --------------------------------------------------- */
	/// Negotiation Callbacks used by Python-hook methods.
	/// these are the only methods that call the Genius Agent.
	/* --------------------------------------------------- */
	private String chooseActionAgent(String agent_uuid, int round) {
		AgentAdapter agent = agents.get(agent_uuid);
		boolean isFirstTurn = first_actions.get(agent_uuid);
		TimeLineInfo timeline = ((Agent)agent).timeline;
		if (is_debug) {
			String msg = String.format("\tRelative time for %s is %f [round %d]\n", agent_uuid, timeline.getTime(), round);
			info(msg);
//			System.out.println(msg);
		}
		List<Class<? extends Action>> validActions = new ArrayList<Class<? extends Action>>();
		if (!isFirstTurn)
			validActions.add(Accept.class);
		validActions.add(EndNegotiation.class);
		validActions.add(Offer.class);
		// System.out.format("Before actual agent chooses action");
		genius.core.actions.Action action = null;
		if (force_timeout) {
			ExecutorWithTimeout executor = executors.get(agent_uuid);
			try {
				action = executor.execute(agent.toString(), new Callable<Action>() {
					@Override
					public Action call() throws Exception {
						return agent.chooseAction(validActions);
					}
				});
			} catch (TimeoutException e) {
				String msg = "Negotiating party " + agent_uuid + " timed out in chooseAction() method.";
				if (logger != null) logger.info(msg);
			} catch (ExecutionException e) {
				String msg = "Negotiating party " + agent_uuid + " threw an exception in chooseAction() method.";
				if (logger != null) logger.info(msg);
			}
		} else {
			action = agent.chooseAction(validActions);
		}
		if (action == null) {
			return (agent != null ? getAgentName(agent_uuid) : "NoAgent") + FIELD_SEP + "NoAction" + FIELD_SEP;
		}
		if (is_debug) {
			String msg = String.format("\t%s -> %s\n", agent_uuid, action.toString());
//			System.out.print(msg);
			info(msg);
//			System.out.flush();
		} else {
			printStatus();
		}
		if (timeline != null && timeline instanceof DiscreteTimeline) {
			if (round< 0)
				((DiscreteTimeline) timeline).increment();
			else
				((DiscreteTimeline) timeline).setcRound(round);
		}
		return actionToString(agent_uuid, action);

	}

	public boolean run_negotiation(String p, String domainFile, String sProfiles, String sAgents, String outputFile) throws Exception {
		List<String> agents = Arrays.asList(sAgents.split(";", -1));
		List<String> profiles = Arrays.asList(sProfiles.split(";", -1));
		return this._run_negotiation(p, domainFile, profiles, agents, outputFile);
	}

	private boolean _run_negotiation(String p, String domainFile, List<String> profiles, List<String> agents, String outputFile) throws Exception {
	    if (!is_silent)
			print_negotiation_info(outputFile, agents, profiles, domainFile, p);
		if (p == null || domainFile==null){
			return false;
		}
		if (profiles.size() != agents.size())
			return false;

		Global.logPreset = outputFile;
		Protocol ns = null;

		ProtocolRepItem protocol = new ProtocolRepItem(p, p, p);

		DomainRepItem dom = new DomainRepItem(new URL(domainFile));

		ProfileRepItem[] agentProfiles = new ProfileRepItem[profiles.size()];
		for (int i = 0; i < profiles.size(); i++) {
			agentProfiles[i] = new ProfileRepItem(new URL(profiles.get(i)), dom);
			if(!is_silent)
				System.out.format("Profile: %s\n", agentProfiles[i].toString());
			if (agentProfiles[i].getDomain() != agentProfiles[0].getDomain())
				throw new IllegalArgumentException("Profiles for agent 0 and agent " + i
						+ " do not have the same domain. Please correct your profiles");
		}

		AgentRepItem[] agentsrep = new AgentRepItem[agents.size()];
		HashMap<AgentParameterVariable, AgentParamValue>[] agentParams = new HashMap[agentProfiles.length];
		for (int i = 0; i < agents.size(); i++) {
			agentsrep[i] = new AgentRepItem(agents.get(i), agents.get(i), agents.get(i));
			agentParams[i] = new HashMap();
			if(!is_silent)
				System.out.format("Agent Type: %s\n", agentsrep[i].toString());
		}

		ns = Global.createProtocolInstance(protocol, agentsrep, agentProfiles, agentParams);

		ns.startSession();
		ns.wait();
		return true;
	}

	private String chooseActionParty(String agent_uuid, int round) {
		// System.out.format("Entered choose action party:");
		AbstractNegotiationParty agent = parties.get(agent_uuid);
		boolean isFirstTurn = first_actions.get(agent_uuid);
		TimeLineInfo timeline = agent.getTimeLine();
		if (is_debug) {
			String msg = String.format("\tRelative time for %s is %f\n", agent_uuid, timeline.getTime());
			info(msg);
//			System.out.println(msg);
		}
		List<Class<? extends Action>> validActions = new ArrayList<Class<? extends Action>>();
		if (!isFirstTurn)
			validActions.add(Accept.class);
		validActions.add(EndNegotiation.class);
		validActions.add(Offer.class);
		// System.out.format("Before actual agent chooses action");
		genius.core.actions.Action action = null;
		if (force_timeout) {
			ExecutorWithTimeout executor = executors.get(agent_uuid);
			try {
				action = executor.execute(agent.toString(), new Callable<Action>() {
					@Override
					public Action call() throws Exception {
						return agent.chooseAction(validActions);
					}
				});
			} catch (TimeoutException e) {
				String msg = "Negotiating party " + agent_uuid + " timed out in chooseAction() method.";
				if (logger != null) logger.info(msg);
			} catch (ExecutionException e) {
				String msg = "Negotiating party " + agent_uuid + " threw an exception in chooseAction() method.";
				if (logger != null) logger.info(msg);
			}
		} else {
			action = agent.chooseAction(validActions);
		}
		if (action == null) {
			return (agent != null ? getAgentName(agent_uuid) : "NoAgent") + FIELD_SEP + "NoAction" + FIELD_SEP;
		}
		// System.out.format("After actual agent chooses action");
		if (is_debug) {
			String msg = String.format("\t%s -> %s\n", agent_uuid, action.toString());
//			System.out.print(msg);
			info(msg);
//			System.out.flush();
		} else {
			printStatus();
		}
		if (timeline != null && timeline instanceof DiscreteTimeline)
			if (round< 0)
				((DiscreteTimeline) timeline).increment();
			else
				((DiscreteTimeline) timeline).setcRound(round);
		return actionToString(agent_uuid, action);
	}

	private Boolean receiveMesasgeAgent(String agent_uuid, String from_id, String typeOfAction, String bid_str, int round) {
		AgentAdapter agent = agents.get(agent_uuid);
		boolean isFirstTurn = first_actions.get(agent_uuid);
		if (isFirstTurn)
			first_actions.put(agent_uuid, false);
		Bid bid = strToBid(agent_uuid, bid_str);
		AgentID agentID = new AgentID(from_id);
		if (is_debug) {
			printStatus();
		} else {
			printStatus();
		}
		TimeLineInfo timeline = ((Agent) agent).timeline;
		if (is_debug) {
			String msg = String.format("\tRelative time for %s (receive) is %f\n", agent_uuid, timeline.getTime());
			info(msg);
		}
		if (timeline != null && timeline instanceof DiscreteTimeline) {
			if (round >= 0)
				((DiscreteTimeline) timeline).setcRound(round);
		}
		final Action act = typeOfAction.contains("Offer") ? new Offer(agentID, bid)
				: typeOfAction.contains("Accept") ? new Accept(agentID, bid)
						: typeOfAction.contains("EndNegotiation") ? new EndNegotiation(agentID) : null;
		// agent.receiveMessage(agentID, act);
		if (force_timeout) {
			ExecutorWithTimeout executor = executors.get(agent_uuid);
			try {
				executor.execute(agent.toString(), new Callable<AgentID>() {
					@Override
					public AgentID call() throws Exception {
						agent.receiveMessage(agentID, act);
						return agentID;
					}
				});
			} catch (TimeoutException e) {
				String msg = "Negotiating party " + agent_uuid + " timed out in receiveMessage() method.";
				if (logger != null) logger.info(msg);
			} catch (ExecutionException e) {
				String msg = "Negotiating party " + agent_uuid + " threw an exception in receiveMessage() method.";
				if (logger != null) logger.info(msg);
			}
		} else {
			agent.receiveMessage(agentID, act);
		}
		if (is_debug && ! is_silent) {
			System.out.flush();
		}
		return true;
	}

	private Boolean receiveMessageParty(String agent_uuid, String from_id, String typeOfAction, String bid_str, int round) {
		AbstractNegotiationParty agent = parties.get(agent_uuid);
		boolean isFirstTurn = first_actions.get(agent_uuid);
		if (isFirstTurn)
			first_actions.put(agent_uuid, false);
		Bid bid = strToBid(agent_uuid, bid_str);
		AgentID agentID = new AgentID(from_id);
		if (is_debug) {
			printStatus();
		} else {
			printStatus();
		}
		TimeLineInfo timeline = agent.getTimeLine();
		if (is_debug) {
			String msg = String.format("\tRelative time for %s (receive) is %f [round %d]\n", agent_uuid, timeline.getTime(), round);
			info(msg);
		}
		if (timeline != null && timeline instanceof DiscreteTimeline) {
			if (round >= 0)
				((DiscreteTimeline) timeline).setcRound(round);
		}
		final Action act = typeOfAction.contains("Offer") ? new Offer(agentID, bid)
				: typeOfAction.contains("Accept") ? new Accept(agentID, bid)
						: typeOfAction.contains("EndNegotiation") ? new EndNegotiation(agentID) : null;
		if (force_timeout) {
			ExecutorWithTimeout executor = executors.get(agent_uuid);
			try {
				executor.execute(agent.toString(), new Callable<AgentID>() {
					@Override
					public AgentID call() throws Exception {
						agent.receiveMessage(agentID, act);
						return agentID;
					}
				});
			} catch (TimeoutException e) {
				String msg = "Negotiating party " + agent_uuid + " timed out in receiveMessage() method.";
				if (logger != null) logger.info(msg);
			} catch (ExecutionException e) {
				String msg = "Negotiating party " + agent_uuid + " threw an exception in receiveMessage() method.";
				if (logger != null) logger.info(msg);
			}
		} else {
			agent.receiveMessage(agentID, act);
		}
//		if (is_debug) {
//			// System.out.format("agent replied\n");
////			System.out.flush();
//		}
		return true;
	}

	/// -------
	/// Helpers
	/// -------
	private HashMap<String, HashMap<String, Value>> initStrValConversion(String agent_uuid, ArrayList<Issue> issues) {
		HashMap<String, HashMap<String, Value>> string2value = new HashMap<String, HashMap<String, Value>>();
		for (Issue issue : issues) {
			String issue_name = issue.toString();
			string2value.put(issue_name, new HashMap<String, Value>());
			List<ValueDiscrete> values = ((IssueDiscrete) issue).getValues();
			for (Value value : values) {
				string2value.get(issue_name).put(value.toString(), value);
			}
		}
		return string2value;
	}

	private NegotiationInfo createNegotiationInfo(String domain_file_name, String utility_file_name, boolean real_time,
			int max_time, long seed, String agent_uuid, long max_time_per_agent) {
		try {
			DomainImpl domain = new DomainImpl(domain_file_name);
			AdditiveUtilitySpace utilSpace = new AdditiveUtilitySpace(domain, utility_file_name);
			TimeLineInfo timeline;
			DeadlineType tp;
			long timeout = global_timeout;
			if (real_time) {
				tp = DeadlineType.TIME;
				timeline = new ContinuousTimeline(max_time);
				if (max_time < timeout) {
					timeout = max_time;
				}
			} else {
				tp = DeadlineType.ROUND;
				timeline = new DiscreteTimeline(max_time);
			}
			if (max_time_per_agent < timeout) {
				timeout = max_time_per_agent;
			}
			Deadline deadline = new Deadline(max_time, tp);
			long randomSeed = seed;
			AgentID agentID = new AgentID(agent_uuid);
			DefaultPersistentDataContainer storage = new DefaultPersistentDataContainer(new Serialize(),
					PersistentDataType.DISABLED);
			NegotiationInfo info = new NegotiationInfo(utilSpace, deadline, timeline, randomSeed, agentID, storage);
			infos.put(agent_uuid, info);
			ids.put(agent_uuid, agentID);
			util_spaces.put(agent_uuid, utilSpace);
			domains.put(agent_uuid, domain);
			timelines.put(agent_uuid, info.getTimeline());
			first_actions.put(agent_uuid, true);
			if (force_any_timeout)
				executors.put(agent_uuid, new ExecutorWithTimeout(timeout));
			ArrayList<Issue> issues = (ArrayList<Issue>) utilSpace.getDomain().getIssues();
			issues_all.put(agent_uuid, issues);
			string2values.put(agent_uuid, this.initStrValConversion(agent_uuid, issues));
			HashMap<String, Issue> striss = new HashMap<String, Issue>();
			for (Issue issue : issues) {
				striss.put(issue.toString(), issue);
			}
			string2issues.put(agent_uuid, striss);
			return info;
		} catch (Exception e) {
			// TODO: handle exception
			if(!is_silent)
				System.out.println(e);
		}
		return null;
	}

	private String getAgentName(String agent_uuid) {
	    try {
			if (this.is_party.get(agent_uuid))
				return parties.get(agent_uuid).getDescription();
			else
				return agents.get(agent_uuid).getDescription();
		} catch(Exception e){
	    	return "UNKNOWN";
		}
	}

	private String actionToString(String agent_uuid, Action action) {
		// System.out.print("Entering action to string");
		String id = action.getAgent().getName();
		Bid bid = null;
		if (action instanceof Offer) {
			bid = ((Offer) action).getBid();
			if (bid == null) {
				// Here we assume that offering None is handled the same way
				// it is done in negmas. It may lead to ending the negotiation
				// or just a new offer
				return id + FIELD_SEP + "NullOffer" + FIELD_SEP;
			}
			List<Issue> issues = bid.getIssues();
			HashMap<Integer, Value> vals = bid.getValues();
			HashMap<String, String> vals_str = new HashMap<String, String>();
			for (Issue issue : issues) {
				Value value = bid.getValue(issue.getNumber());
				vals_str.put(issue.toString(), value.toString());
			}
			// for (Integer key : vals.keySet()) {
			// vals_str.put(issues.get(key).toString(), vals.get(key).toString());
			// }

			String bidString = MapUtil.mapToString(vals_str, ENTRY_SEP, INTERNAL_SEP);
			return id + FIELD_SEP + "Offer" + FIELD_SEP + bidString;
		} else if (action instanceof EndNegotiation)
			return id + FIELD_SEP + "EndNegotiation" + FIELD_SEP;
		else if (action instanceof Accept)
			return id + FIELD_SEP + "Accept" + FIELD_SEP;
		return id + FIELD_SEP + "Failure" + FIELD_SEP;
	}

	private Bid strToBid(String agent_uuid, String bid_str) {
		AbstractUtilitySpace utilSpace = util_spaces.get(agent_uuid);
//		ArrayList<Issue> issues = issues_all.get(agent_uuid);
		// Bid bid = new Bid(utilSpace.getDomain());//.getRandomBid(new Random());
		if (bid_str == null) {
			String msg = String.format("Received null bid ID %s", agent_uuid);
//			System.out.print(msg);
			info(msg);
//			System.out.flush();
			return null;
		}
		if (bid_str.equals("")) {
			String msg = String.format("Received empty bid ID %s", agent_uuid);
//			System.out.print(msg);
			info(msg);
//			System.out.flush();
			return null;
		}
		String[] bid_strs = bid_str.split(ENTRY_SEP);
		HashMap<Integer, Value> vals = new HashMap<Integer, Value>();
		for (String str : bid_strs) {
			String[] vs = str.split(INTERNAL_SEP);
			String issue_name = vs[0];
			String val = vs.length > 1 ? vs[1] : "";
			vals.put(string2issues.get(agent_uuid).get(issue_name).getNumber(),
					string2values.get(agent_uuid).get(issue_name).get(val));
		}
		Bid bid = null;
		try {
			bid = new Bid(utilSpace.getDomain(), vals);
		}catch(Exception e){
			bid = null;
			warning(e.toString());
		}
		return bid;
	}

	private void printStatus() {
		if(is_silent)
		    return;
		System.out.format("\r%06d agents (%06d active) [%06d ids]: %09d received, %09d sent", n_total_agents,
				n_active_agents, this.ids.size(), n_total_offers, n_total_responses);
		System.out.flush();
	}

	private void info(String s) {
		if (logging)
			logger.info(s);
	}

	private void warning(String s) {
		if (logging)
			logger.warning(s);
	}

	private void error(String s) {
		if (logging)
			logger.log(Level.SEVERE, s);
	}

	/// ----------------------
	/// Py4J server Management
	/// ----------------------
	public void setPy4jServer(GatewayServer server) {
		info(String.format("Py4j Server is set"));
		this.server = server;
	}

	public void startPy4jServer(int port) {
		info(String.format("Starting Py4j Server at port %d", port));
		server = new GatewayServer(this, port);
		try {
			server.start();
		} catch (Py4JNetworkException e) {
			error(String.format("Failed to start a bridge at port %d: %s", port, e.toString()));
			System.exit(-1);
		}
		int listening_port = server.getListeningPort();
		String msg = String.format("Gateway v0.11 to python started at port %d listening to port %d [%s: %d]\n", port,
				listening_port, force_timeout ? "forcing timeout" : "no  timeout", this.global_timeout);
		if(!is_silent)
			System.out.print(msg);
		info(msg);
	}

	public void shutdownPy4jServer() {
		server.shutdown();
	}

	public static void main(String[] args) {
		int port = 25337;
		long timeout = 3 * 60;
		boolean is_debug = false;
		boolean is_silent = true;
		boolean dieOnBrokenPipe = false;
		boolean force_timeout = true;
		boolean force_timeout_init = false;
		boolean force_timeout_end = false;
		boolean logging = false;
		String logFile = "genius-bridge-log.txt";
		String s = String.format("received options: ");
		boolean run_neg = false;
		List<String> agents = new ArrayList<>();
		List<String> profiles = new ArrayList<>();
		String domainFile = null, protocol = null, outputFile = null;
		for (int i = 0; i < args.length; i++) {
			String opt = args[i];
			s += String.format("%s ", opt);
			if (run_neg){
			    if (opt.startsWith("-u") || opt.startsWith("--profile") || opt.startsWith("--ufun")){
			    	profiles.add(String.format("file://%s", opt.split("=")[1]));
				}
				if (opt.startsWith("-a") || opt.startsWith("--agent") || opt.startsWith("-n") || opt.startsWith("--negotiator")){
					agents.add(String.format("file://%s", opt.split("=")[1]));
				}
				if (opt.startsWith("-p") || opt.startsWith("--protocol") || opt.startsWith("--mechanism")){
					protocol = opt.split("=")[1];
				}
				if (opt.startsWith("-d") || opt.startsWith("--domain") || opt.startsWith("--issues")){
					domainFile = String.format("file://%s", opt.split("=")[1]);
				}
				if (opt.startsWith("-o") || opt.startsWith("--log") || opt.startsWith("--output")){
					outputFile = String.format("file://%s", opt.split("=")[1]);
				}
				continue;
			}
			if (opt.equals("run")) {
				run_neg = true;
				s = "";
			} else if (opt.equals("--die-on-exit") || opt.equals("die-on-exit")) {
				dieOnBrokenPipe = true;
			} else if (opt.equals("--debug") || opt.equals("debug")) {
				is_debug = true;
				is_silent = false;
				logging = true;
			} else if (opt.equals("--silent") || opt.equals("silent")) {
				is_silent = true;
			} else if (opt.equals("--verbose") || opt.equals("verbose")) {
				is_silent = false;
			} else if (opt.startsWith("--timeout") || opt.equals("timeout")) {
				timeout = Integer.parseInt(opt.split("=")[1]);
			} else if (opt.startsWith("--logfile") || opt.equals("log-file")) {
				logFile = opt.split("=")[1];
			} else if (opt.startsWith("--force-timeout") || opt.equals("force-timeout")) {
				force_timeout = true;
			} else if (opt.startsWith("--no-timeout") || opt.equals("no-timeout")) {
				force_timeout = false;
			} else if (opt.startsWith("--force-timeout-init") || opt.equals("force-timeout-init")) {
				force_timeout_init = true;
			} else if (opt.startsWith("--no-timeout-init") || opt.equals("no-timeout-init")) {
				force_timeout_init = false;
			} else if (opt.startsWith("--force-timeout-end") || opt.equals("force-timeout-end")) {
				force_timeout_end = true;
			} else if (opt.startsWith("--no-timeout-end") || opt.equals("no-timeout-end")) {
				force_timeout_end = false;
			} else if (opt.startsWith("--no-logs") || opt.equals("no-logs")) {
				logging = false;
			} else if (opt.startsWith("--with-logs") || opt.equals("with-logs")) {
				logging = true;
			} else {
				port = Integer.parseInt(opt);
			}
		}
		if (run_neg) {
			NegLoader n = new NegLoader();
			boolean result = false;
			try {
				result = n._run_negotiation(protocol, domainFile, profiles, agents, outputFile);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (result) {
				if (!is_silent)
					System.out.println("Negotiation Succeeded");
			} else {
				if (!is_silent)
					System.out.println("Negotiation FAILED");
			}
			return;
		}
		if (!force_timeout) {
			timeout = -1;
		}
		Logger logger = null;
		if (logging){
			logger = Logger.getLogger("com.yasserm.geniusbridge");
			Handler fh = null;
			try {
				fh = new FileHandler(logFile);
				logger.addHandler(fh);
				SimpleFormatter formatter = new SimpleFormatter();
				fh.setFormatter(formatter);
				logger.setLevel(Level.INFO);
				if (logging) {
					logger.info("Genius Bridge STARTED");
					logger.info(s);
				}
			} catch (IOException e) {
				e.printStackTrace();
				logging = false;
				logger = null;
			}
		}
		if (!is_silent) {
			System.out.println(s);
			System.out.flush();
		}
		NegLoader app = new NegLoader(is_debug, force_timeout, force_timeout_init, force_timeout_end, timeout, logging,
				logging? logger: null, is_silent);
		app.info("NegLoader object is constructed");
		app.startPy4jServer(port);
		app.info(String.format("Py4j server is started at port %d", port));
		if (dieOnBrokenPipe) {
			/*
			 * Exit on EOF or broken pipe. This ensures that the server dies if its parent
			 * program dies.
			 */
			try {
				BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in, Charset.forName("UTF-8")));
				stdin.readLine();
				System.exit(0);
			} catch (java.io.IOException e) {
				System.exit(1);
			}
		}
	}

	private static void print_negotiation_info(String outputFile, List<String> agents, List<String> profiles, String domainFile, String protocol) {
		System.out.format("Running negotiation of type %s\nDomain: %s\nLog: %s\nProfiles:\n", protocol, domainFile, outputFile);
		for (String profile: profiles) {
			System.out.format("\t%s\n", profile);
		}
		System.out.println("Agents:");
		for (String agent: agents) {
			System.out.format("\t%s\n", agent);
		}
	}
}
