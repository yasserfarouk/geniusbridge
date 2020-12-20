package com.yasserm.negmasgeniusbridge;

import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.io.FileInputStream;
import java.lang.Class;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import py4j.GatewayServer;
import java.io.Serializable;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import genius.core.AgentID;
import genius.core.Bid;
import genius.core.Deadline;
import genius.core.AgentAdapter;
import genius.core.DeadlineType;
import genius.core.Domain;
import genius.core.DomainImpl;
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
import java.util.concurrent.ThreadLocalRandom;

import java.util.Map;

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
	public boolean is_debug = false;
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

	private HashMap<String, ArrayList<Issue>> issues_all;
	private HashMap<String, HashMap<String, HashMap<String, Value>>> string2values = null;
	private HashMap<String, HashMap<String, Issue>> string2issues = null;
	private HashMap<String, TimeLineInfo> timelines = null;

	private int n_agents = 0;

	private String INTERNAL_SEP = "<<s=s>>";
	private String ENTRY_SEP = "<<y,y>>";
	private String FIELD_SEP = "<<sy>>";

	public class Serialize implements Serializable {
	}

	public NegLoader(boolean is_debug) {
		this.is_debug = is_debug;
		parties = new HashMap<String, AbstractNegotiationParty>();
		agents = new HashMap<String, AgentAdapter>();
		is_party = new HashMap<String, Boolean>();
		infos = new HashMap<String, NegotiationInfo>();
		ids = new HashMap<String, AgentID>();
		domains = new HashMap<String, Domain>();
		util_spaces = new HashMap<String, AdditiveUtilitySpace>();
		first_actions = new HashMap<String, Boolean>();
		string2values = new HashMap<String, HashMap<String, HashMap<String, Value>>>();
		issues_all = new HashMap<String, ArrayList<Issue>>();
		timelines = new HashMap<String, TimeLineInfo>();
		string2issues = new HashMap<String, HashMap<String, Issue>>();
	}

	public String test(String class_name) {
		ArrayList classes = new ArrayList();

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

	private NegotiationInfo createNegotiationInfo(String domain_file_name, String utility_file_name, boolean real_time,
			int max_time, long seed, String agent_uuid) {
		try {
			DomainImpl domain = new DomainImpl(domain_file_name);
			AdditiveUtilitySpace utilSpace = new AdditiveUtilitySpace(domain, utility_file_name);
			TimeLineInfo timeline;
			DeadlineType tp;
			if (real_time) {
				tp = DeadlineType.TIME;
				timeline = new ContinuousTimeline(max_time);
			} else {
				tp = DeadlineType.ROUND;
				timeline = new DiscreteTimeline(max_time);
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
			timelines.put(agent_uuid, timeline);
			first_actions.put(agent_uuid, true);
			ArrayList<Issue> issues = (ArrayList<Issue>) utilSpace.getDomain().getIssues();
			issues_all.put(agent_uuid, issues);
			string2values.put(agent_uuid, this.init_str_val_conversion(agent_uuid, issues));
			HashMap<String, Issue> striss = new HashMap<String, Issue>();
			for (Issue issue : issues) {
				striss.put(issue.toString(), issue);
			}
			string2issues.put(agent_uuid, striss);
			return info;
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}
		return null;
	}

	public void on_negotiation_start(String agent_uuid, int n_agents, long n_steps, long time_limit, boolean real_time,
			String domain_file_name, String utility_file_name) {
		this.n_agents = n_agents;
		if (is_party.get(agent_uuid)) {
			AbstractNegotiationParty agent = this.parties.get(agent_uuid);
			int seed = is_debug ? ThreadLocalRandom.current().nextInt(0, 10000) : 0;
			NegotiationInfo info = createNegotiationInfo(domain_file_name, utility_file_name, real_time,
					real_time ? (int) time_limit : (int) n_steps, seed, agent_uuid);
			if (info == null)
				return;
			agent.init(info);
		} else {
			AgentAdapter agent = this.agents.get(agent_uuid);
			NegotiationInfo info = createNegotiationInfo(domain_file_name, utility_file_name, real_time,
					real_time ? (int) time_limit : (int) n_steps, 0, agent_uuid);
			if (info == null)
				return;
			agent.init(info);
		}
		n_total_negotiations++;
		n_active_negotiations++;
		if (is_debug) {
			System.out.format("Agent %s: time limit %d, step limit %d\n", getName(agent_uuid), time_limit, n_steps);
			System.out.flush();
		} else {
			print_status();
		}
	}

	private void print_status() {
		System.out.format("\r%06d agents (%06d active) : %09d received, %09d sent", n_total_agents, n_active_agents,
				n_total_offers, n_total_responses);
		System.out.flush();
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
				System.out.format("Creating Agent of type %s (ID= %s)\n", class_name, uuid);
				System.out.flush();
			} else {
				print_status();
			}
			return uuid;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}

	public void on_negotiation_end(String agent_uuid, String bid_str) {
		Bid bid = bid_str == null ? null : str2bid(agent_uuid, bid_str);
		if (is_party.get(agent_uuid)) {
			AbstractNegotiationParty agent = (AbstractNegotiationParty) parties.get(agent_uuid);
			agent.negotiationEnded(bid);
		} else {
			AgentAdapter agent = (AgentAdapter) agents.get(agent_uuid);
			agent.negotiationEnded(bid);
		}
	}

	public String destroy_agent(String agent_uuid) {
		this.infos.remove(agent_uuid);
		this.ids.remove(agent_uuid);
		this.util_spaces.remove(agent_uuid);
		this.domains.remove(agent_uuid);
		this.timelines.remove(agent_uuid);
		this.first_actions.remove(agent_uuid);
		this.string2values.remove(agent_uuid);
		this.string2issues.remove(agent_uuid);
		this.issues_all.remove(agent_uuid);
		if (is_party.get(agent_uuid)) {
			this.parties.remove(agent_uuid);
		} else {
			this.agents.remove(agent_uuid);
		}
		n_active_agents--;
		if (is_debug) {
			System.out.format("Agent %s destroyed\n", agent_uuid);
			System.out.flush();
		} else {
			print_status();
		}
		return "";
	}

	public String actionToString(String agent_uuid, Action action) {
		// System.out.print("Entering action to string");
		String id = action.getAgent().getName();
		Bid bid = null;
		if (action instanceof Offer) {
			bid = ((Offer) action).getBid();
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

	public String choose_action(String agent_uuid) {
		n_total_responses++;
		// System.out.format("Entered choose actino");
		if (is_party.get(agent_uuid))
			return choose_action_party(agent_uuid);
		return choose_action_agent(agent_uuid);
	}

	public String choose_action_agent(String agent_uuid) {
		AgentAdapter agent = agents.get(agent_uuid);
		boolean isFirstTurn = first_actions.get(agent_uuid);
		TimeLineInfo timeline = timelines.get(agent_uuid);
		List<Class<? extends Action>> validActions = new ArrayList<Class<? extends Action>>();
		if (!isFirstTurn)
			validActions.add(Accept.class);
		validActions.add(EndNegotiation.class);
		validActions.add(Offer.class);
		// System.out.format("Before actual agent chooses action");
		genius.core.actions.Action action = agent.chooseAction(validActions);
		// System.out.format("After actual agent chooses action");
		if (is_debug) {
			System.out.format("\t%s -> %s\n", agent_uuid, action.toString());
			System.out.flush();
		} else {
			print_status();
		}
		if (timeline instanceof DiscreteTimeline)
			((DiscreteTimeline) timeline).increment();
		return actionToString(agent_uuid, action);
		// boolean isFirstTurn = first_actions.get(agent_uuid);
		// TimeLineInfo timeline = timelines.get(agent_uuid);
		// List<Class<? extends Action>> validActions = new ArrayList<Class<? extends
		// Action>>();
		// if(!isFirstTurn)
		// validActions.add(Accept.class);
		// validActions.add(EndNegotiation.class);
		// validActions.add(Offer.class);
		// genius.core.actions.Action action = agent.chooseAction(validActions);
		// if(is_debug){
		// System.out.format("\t%s -> %s\n", agent_uuid, action.toString());
		// System.out.flush();
		// }else {
		// print_status();
		// }
		// if(timeline instanceof DiscreteTimeline)
		// ((DiscreteTimeline) timeline).increment();
		// return actionToString(action);

	}

	public String choose_action_party(String agent_uuid) {
		// System.out.format("Entered choose action party:");
		AbstractNegotiationParty agent = parties.get(agent_uuid);
		boolean isFirstTurn = first_actions.get(agent_uuid);
		TimeLineInfo timeline = timelines.get(agent_uuid);
		List<Class<? extends Action>> validActions = new ArrayList<Class<? extends Action>>();
		if (!isFirstTurn)
			validActions.add(Accept.class);
		validActions.add(EndNegotiation.class);
		validActions.add(Offer.class);
		// System.out.format("Before actual agent chooses action");
		genius.core.actions.Action action = agent.chooseAction(validActions);
		// System.out.format("After actual agent chooses action");
		if (is_debug) {
			System.out.format("\t%s -> %s\n", agent_uuid, action.toString());
			System.out.flush();
		} else {
			print_status();
		}
		if (timeline instanceof DiscreteTimeline)
			((DiscreteTimeline) timeline).increment();
		return actionToString(agent_uuid, action);
	}

	public Boolean receive_message(String agent_uuid, String from_id, String typeOfAction, String bid_str) {
		n_total_offers++;
		boolean result;
		if (is_party.get(agent_uuid))
			result = receive_message_party(agent_uuid, from_id, typeOfAction, bid_str);
		else
			result = receive_mesasge_agent(agent_uuid, from_id, typeOfAction, bid_str);
		return result;
	}

	public Boolean receive_mesasge_agent(String agent_uuid, String from_id, String typeOfAction, String bid_str) {
		AgentAdapter agent = agents.get(agent_uuid);
		boolean isFirstTurn = first_actions.get(agent_uuid);
		if (isFirstTurn)
			first_actions.put(agent_uuid, false);
		Bid bid = str2bid(agent_uuid, bid_str);
		AgentID agentID = new AgentID(from_id);
		Action act = null;
		if (typeOfAction.contains("Offer")) { // "Offer"
			act = new Offer(agentID, bid);
		} else if (typeOfAction.contains("Accept")) {
			act = new Accept(agentID, bid);
		} else if (typeOfAction.contains("EndNegotiation"))
			act = new EndNegotiation(agentID);
		if (is_debug) {
			// System.out.format("\t%s <- %s ", agent_uuid, act);
			// System.out.flush();
			print_status();
		} else {
			print_status();
		}
		agent.receiveMessage(agentID, act);
		if (is_debug) {
			// System.out.format("agent replied\n");
			System.out.flush();
		}
		return true;
		// boolean isFirstTurn = first_actions.get(agent_uuid);
		// if(isFirstTurn)
		// first_actions.put(agent_uuid, false);
		// Bid bid = str2bid(agent_uuid, bid_str);
		// AgentID agentID = new AgentID(from_id);
		// Action act = null;
		// if (typeOfAction.contains("Offer")){ // "Offer"
		// act = new Offer(agentID, bid);
		// }
		// else if(typeOfAction.contains("Accept")){
		// act = new Accept(agentID, bid);
		// }
		// else if(typeOfAction.contains("EndNegotiation"))
		// act = new EndNegotiation(agentID);
		// agent.ReceiveMessage(act);
		// if(is_debug){
		// System.out.format("\t%s <- %s\n", agent_uuid, act);
		// System.out.flush();
		// }else {
		// print_status();
		// }
		// return true;
	}

	public Boolean receive_message_party(String agent_uuid, String from_id, String typeOfAction, String bid_str) {
		AbstractNegotiationParty agent = parties.get(agent_uuid);
		boolean isFirstTurn = first_actions.get(agent_uuid);
		if (isFirstTurn)
			first_actions.put(agent_uuid, false);
		Bid bid = str2bid(agent_uuid, bid_str);
		AgentID agentID = new AgentID(from_id);
		Action act = null;
		if (typeOfAction.contains("Offer")) { // "Offer"
			act = new Offer(agentID, bid);
		} else if (typeOfAction.contains("Accept")) {
			act = new Accept(agentID, bid);
		} else if (typeOfAction.contains("EndNegotiation"))
			act = new EndNegotiation(agentID);
		if (is_debug) {
			// System.out.format("\t%s <- %s ", agent_uuid, act);
			// System.out.flush();
			print_status();
		} else {
			print_status();
		}
		agent.receiveMessage(agentID, act);
		if (is_debug) {
			// System.out.format("agent replied\n");
			System.out.flush();
		}
		return true;
	}

	public HashMap<String, HashMap<String, Value>> init_str_val_conversion(String agent_uuid, ArrayList<Issue> issues) {
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

	public Bid str2bid(String agent_uuid, String bid_str) {
		AbstractUtilitySpace utilSpace = util_spaces.get(agent_uuid);
		ArrayList<Issue> issues = issues_all.get(agent_uuid);
		// Bid bid = new Bid(utilSpace.getDomain());//.getRandomBid(new Random());
		if (bid_str == null) {
			System.out.format("Received null bid ID %s", agent_uuid);
			System.out.flush();
			return null;
		}
		if (bid_str.equals("")) {
			System.out.format("Received empty bid ID %s", agent_uuid);
			System.out.flush();
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
		return new Bid(utilSpace.getDomain(), vals);
	}

	public void informMessage(String agent_uuid, int agent_num) {
		Inform inform = new Inform(ids.get(agent_uuid), "NumberOfAgents", agent_num);
		parties.get(agent_uuid).receiveMessage(ids.get(agent_uuid), inform);
	}

	public void informMessage(String agent_uuid) {
		Inform inform = new Inform(ids.get(agent_uuid), "NumberOfAgents", this.n_agents);
		parties.get(agent_uuid).receiveMessage(ids.get(agent_uuid), inform);
	}

	public String getName(String agent_uuid) {
		if (this.is_party.get(agent_uuid))
			return parties.get(agent_uuid).getDescription();
		else
			return agents.get(agent_uuid).getDescription();
	}

	public static void main(String[] args) {
		int port = 25337;
		boolean is_debug = false;
		boolean dieOnBrokenPipe = false;
		System.out.format("Received options: ");
		for (int i = 0; i < args.length; i++) {
			String opt = args[i];
			System.out.format("%s ", opt);
			if (opt.equals("--die-on-exit") || opt.equals("die-on-exit")) {
				dieOnBrokenPipe = true;
			} else if (opt.equals("--debug") || opt.equals("debug")) {
				is_debug = true;
			} else {
				port = Integer.parseInt(opt);
			}
		}
		System.out.format("\n");
		NegLoader app = new NegLoader(is_debug);
		// app is now the gateway.entry_point
		GatewayServer server = new GatewayServer(app, port);
		server.start();
		int listening_port = server.getListeningPort();
		System.out.format("Gateway v0.1 to python started at port %d listening to port %d\n", port, listening_port);

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
}
