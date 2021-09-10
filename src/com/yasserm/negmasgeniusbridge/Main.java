package com.yasserm.negmasgeniusbridge;

import genius.core.*;
import genius.core.actions.*;
import genius.core.issue.Issue;
import genius.core.issue.IssueDiscrete;
import genius.core.issue.Value;
import genius.core.issue.ValueDiscrete;
import genius.core.parties.AbstractNegotiationParty;
import genius.core.parties.NegotiationInfo;
import genius.core.persistent.DefaultPersistentDataContainer;
import genius.core.persistent.PersistentDataType;
import genius.core.protocol.Protocol;
import genius.core.repository.AgentRepItem;
import genius.core.repository.DomainRepItem;
import genius.core.repository.ProfileRepItem;
import genius.core.repository.ProtocolRepItem;
import genius.core.session.ExecutorWithTimeout;
import genius.core.timeline.ContinuousTimeline;
import genius.core.timeline.DiscreteTimeline;
import genius.core.timeline.TimeLineInfo;
import genius.core.tournament.VariablesAndValues.AgentParamValue;
import genius.core.tournament.VariablesAndValues.AgentParameterVariable;
import genius.core.utility.AbstractUtilitySpace;
import genius.core.utility.AdditiveUtilitySpace;
import py4j.GatewayServer;
import py4j.Py4JNetworkException;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.logging.Logger;
import java.util.logging.*;


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
        }

        return stringBuilder.toString();
    }

    /*
    public static Map<String, String> stringToMap(String input, String entry_separator, String internal_separator) {
        Map<String, String> map = new HashMap<>();

        String[] nameValuePairs = input.split(entry_separator);
        for (String nameValuePair : nameValuePairs) {
            String[] nameValue = nameValuePair.split(internal_separator);
            map.put(nameValue[0], nameValue.length > 1 ? nameValue[1] : "");
        }

        return map;
    }
     */
}

class NegLoader {
    final public boolean is_debug;
    final private HashMap<String, AbstractNegotiationParty> parties = new HashMap<>();
    final private HashMap<String, AgentAdapter> agents = new HashMap<>();
    final private HashMap<String, Boolean> is_party = new HashMap<>();
    final private HashMap<String, AgentID> ids = new HashMap<>();
    final private HashMap<String, Boolean> is_strict = new HashMap<>();
    final private HashMap<String, Boolean> isRealTimeLimit = new HashMap<>();
    final private HashMap<String, AdditiveUtilitySpace> util_spaces = new HashMap<>();
    final private HashMap<String, Boolean> first_actions = new HashMap<>();
    final private HashMap<String, HashMap<String, HashMap<String, Value>>> string2values = new HashMap<>();
    final private HashMap<String, HashMap<String, Issue>> string2issues = new HashMap<>();
    final private String INTERNAL_SEP = "<<s=s>>";
    final private String ENTRY_SEP = "<<y,y>>";
    final private String FIELD_SEP = "<<sy>>";
    final private boolean force_timeout;
    final private boolean force_any_timeout;
    final private boolean force_timeout_init;
    final private boolean force_timeout_end;
    final private Logger logger;
    final private boolean logging;
    final private boolean is_silent;
    public String jarName = "genius-8.0.4-jar-with-dependencies.jar";
    private long n_total_agents = 0;
    private long n_total_negotiations = 0;
    private long n_active_negotiations = 0;
    private long n_active_agents = 0;
    private long n_total_offers = 0;
    private long n_total_responses = 0;
    private HashMap<String, ExecutorWithTimeout> executors;
    private GatewayServer server;
    private int n_agents = 0;
    private long global_timeout = 180;

    public NegLoader() {
        this(false, false, false, false, 0, true, null, true);
    }

    /**
     * @param is_debug           Enable debug mode with more printing and logging
     * @param force_timeout      If given, timeouting will be enforced. Note that this is only effective for agents that are created with a ContinuousTimeline (i.e. finite negmas time_limit)
     * @param force_timeout_init If given, timeouting is enforced in `on_negotiation_start`
     * @param force_timeout_end  If given, timeouting is enforced in `on_negotaition_end`
     * @param timeout            The timeout in seconds
     * @param logging            If True, logging is enabled
     * @param logger             If given and logging==true, the logger to use
     * @param is_silent          If given, no printing to the screen is allowed
     */
    public NegLoader(boolean is_debug, boolean force_timeout, boolean force_timeout_init, boolean force_timeout_end,
                     long timeout, boolean logging, Logger logger, boolean is_silent) {
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
        if (force_any_timeout)
            executors = new HashMap<>();
    }

    /**
     * The main method. Accepts the following arguments:
     * Number: port number to use
     * --version/version: Prints version and exits
     * --run runs a negotiation inside genius (negmas is not involved). In this case these extra paramters are allowed:
     * -u/--profile/--ufun A ufun file
     * -a/--agent/-n/--negotiator A java negotiator (Genius Agent/Party)
     * -p/--protocol/--mechanism the mechanism to use
     * -d/--domain/--issues The domain file
     * -o/--log/--output The output file to keep logs
     * --die-on-exit  Kill the Py4J listener on exit
     * --debug More debug information (implicitly sets logging to true and verbose. This can be overriden by later flags).
     * --silent Silent mode (no printing)
     * --verbose Print progress to screen
     * --timeout The timeout used in seconds (only effective if --force-timeout)
     * --force-timeout If given, timeout is enforced
     * --no-timeout If given, timeout is not enforced
     * --no-timeout-init If given, timeout does not apply to the init() call (in on_negotiation_start)
     * --no-timeout-end If given , timeout does not apply to the on_negotiation_end
     * --no-logs Do not keep logs
     * --with-logs Keep logs
     */
    public static void main(String[] args) {
        int port = 25337;
        long timeout = 3 * 60000;
        boolean is_debug = false;
        boolean is_silent = true;
        boolean dieOnBrokenPipe = false;
        boolean force_timeout = true;
        boolean force_timeout_init = false;
        boolean force_timeout_end = false;
        boolean logging = false;
        String logFile = "genius-bridge-log.txt";
        StringBuilder s = new StringBuilder("received options: ");
        boolean run_neg = false;
        List<String> agents = new ArrayList<>();
        List<String> profiles = new ArrayList<>();
        String domainFile = null, protocol = null, outputFile = null;
        for (String opt : args) {
            s.append(String.format("%s ", opt));
            if (opt.startsWith("--version") || opt.startsWith("version")) {
                System.out.println(version());
                System.exit(0);
            }
            if (run_neg) {
                if (opt.startsWith("-u") || opt.startsWith("--profile") || opt.startsWith("--ufun")) {
                    profiles.add(String.format("file://%s", opt.split("=")[1]));
                }
                if (opt.startsWith("-a") || opt.startsWith("--agent") || opt.startsWith("-n") || opt.startsWith("--negotiator")) {
                    agents.add(String.format("file://%s", opt.split("=")[1]));
                }
                if (opt.startsWith("-p") || opt.startsWith("--protocol") || opt.startsWith("--mechanism")) {
                    protocol = opt.split("=")[1];
                }
                if (opt.startsWith("-d") || opt.startsWith("--domain") || opt.startsWith("--issues")) {
                    domainFile = String.format("file://%s", opt.split("=")[1]);
                }
                if (opt.startsWith("-o") || opt.startsWith("--log") || opt.startsWith("--output")) {
                    outputFile = String.format("file://%s", opt.split("=")[1]);
                }
                continue;
            }
            if (opt.equals("run")) {
                run_neg = true;
                s = new StringBuilder();
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
        if (logging) {
            logger = Logger.getLogger("com.yasserm.geniusbridge");
            Handler fh;
            try {
                fh = new FileHandler(logFile);
                logger.addHandler(fh);
                SimpleFormatter formatter = new SimpleFormatter();
                fh.setFormatter(formatter);
                logger.setLevel(Level.INFO);
                logger.info("Genius Bridge STARTED");
                logger.info(s.toString());
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
                logging ? logger : null, is_silent);
        app.info("NegLoader object is constructed");
        app.startPy4jServer(port);
        app.info(String.format("Py4j server is started at port %d", port));
        if (dieOnBrokenPipe) {
            /*
             * Exit on EOF or broken pipe. This ensures that the server dies if its parent
             * program dies.
             */
            try {
                BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
                stdin.readLine();
                System.exit(0);
            } catch (java.io.IOException e) {
                System.exit(1);
            }
        }
    }

    /**
     * Prints negotiation information to the screen.
     *
     * @param outputFile the file name to keep logs
     * @param agents     Agent classes
     * @param profiles   Ufuns (profiles) as filenames
     * @param domainFile domain file name
     * @param protocol   The negotiation mechanism name
     */
    private static void print_negotiation_info(String outputFile, List<String> agents, List<String> profiles, String domainFile, String protocol) {
        System.out.format("Running negotiation of type %s\nDomain: %s\nLog: %s\nProfiles:\n", protocol, domainFile, outputFile);
        for (String profile : profiles) {
            System.out.format("\t%s\n", profile);
        }
        System.out.println("Agents:");
        for (String agent : agents) {
            System.out.format("\t%s\n", agent);
        }
    }

    /**
     * @return version
     */
    private static String version() {
        return "v0.15";
    }

    /// Python Hooks: Methods called from python (they all have python snake_case
    /// naming convention)

    /**
     * Tests that it is possible to create an object in the JVM
     *
     * @param class_name The class of the object to be created
     * @return Description of the object instance
     */
    public String test(String class_name) {
        info(String.format("Test is called with class-name %s", class_name));
        ArrayList classes = new ArrayList();

        if (!is_silent)
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
                    classes.add(jarEntry.getName().replaceAll("/", "\\."));
                }
            }
            Class<?> clazz = Class.forName(class_name);
            if (!is_silent)
                System.out.println(clazz.toString());
            AbstractNegotiationParty instance = (AbstractNegotiationParty) clazz.newInstance();
            return instance.getDescription();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Creates a new agent (negmas Negotaitor)
     *
     * @param class_name The class name of the negotiator to create
     * @return If successful the UUID of the object created otherwise an empty string
     */
    public String create_agent(String class_name) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        try {
            Class<?> clazz = Class.forName(class_name);
            String uuid = class_name + UUID.randomUUID().toString();
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
                info(String.format("Creating Agent of type %s (ID= %s)\n", class_name, uuid));
            } else {
                this.printStatus();
            }
            return uuid;
        } catch (Exception e) {
            if (!is_silent) e.printStackTrace();
            throw e;
        }
    }

    /**
     * Gets the relative time as seen by the Genius agent
     *
     * @param agent_uuid Agent UUID
     * @return relative time [0-1] of the negotiation (works both for n_steps and time_limit limited negotiations)
     */
    public double get_time(String agent_uuid) throws InvalidObjectException {
        Boolean isparty;
        try {
            isparty = is_party.get(agent_uuid);
        } catch (Exception e) {
            throw new InvalidObjectException(String.format("%s agent was not found. Cannot get_time", agent_uuid));
        }
        if (isparty == null)
            return -1;
        if (isparty) {
            AbstractNegotiationParty agent = parties.get(agent_uuid);
            TimeLineInfo timeline = agent.getTimeLine();
            if (timeline == null)
                return -2;
            return timeline.getTime();
        } else {
            Agent agent = (Agent) agents.get(agent_uuid);
            if (agent.timeline == null)
                return -3;
            return agent.timeline.getTime();
        }
    }

    /***
     * Called by negmas to start a negotiation
     * @param agent_uuid The agent (which must have already been created using `create_agent`)
     * @param n_agents Number of agents in the negotiation
     * @param n_steps number of steps (rounds) of the negotiation
     * @param time_limit number of allowed seconds (either n_steps or time_limit must be given but not both)
     * @param real_time  If true uses time_limit (ContinuousTimeline) else n_steps (DiscreteTimeLimit)
     * @param domain_file_name Domain file name
     * @param utility_file_name Preferences file name (ufun)
     * @param agent_timeout Time out for this agent
     * @param strict If given the bridge will just pass any exceptions to negmas (as well as failure of agents to choose an action)
     * @throws TimeoutException
     * @throws ExecutionException
     */
    public void on_negotiation_start(String agent_uuid, int n_agents, long n_steps, long time_limit, boolean real_time,
                                     String domain_file_name, String utility_file_name, long agent_timeout, boolean strict) throws TimeoutException, ExecutionException, InvalidObjectException {
        this.n_agents = n_agents;
        Boolean isparty = is_party.get(agent_uuid);
        if (isparty == null) {
            if (is_debug) {
                String msg = String.format("Agent %s does not exist", agent_uuid);
                info(msg);
            }
            throw new InvalidObjectException(String.format("%s agent was not found. Cannot start negotiation ", agent_uuid));
        }
        if (is_debug) {
            info(String.format("Domain file: %s\nUfun: %s\nAgent: %s", domain_file_name, utility_file_name, agent_uuid));
        }
        if (isparty) {
            AbstractNegotiationParty agent = this.parties.get(agent_uuid);
            int seed = is_debug ? ThreadLocalRandom.current().nextInt(0, 10000) : 0;
            NegotiationInfo info = createNegotiationInfo(domain_file_name, utility_file_name, real_time,
                    real_time ? (int) time_limit : (int) n_steps, seed, agent_uuid, agent_timeout, strict);
            if (info == null) {
                if (strict)
                    throw new InvalidObjectException(String.format("Cannot create NegotiationInfo for agent %s. Cannot start negotiation ", agent_uuid));
                return;
            }
            if (force_timeout_init) {
                ExecutorWithTimeout executor = executors.get(agent_uuid);
                try {
                    executor.execute(agent_uuid, () -> {
                        agent.init(info);
                        return agent;
                    });
                } catch (TimeoutException e) {
                    String msg = "Negotiating party " + agent_uuid + " timed out in init() method.";
                    if (logger != null) logger.info(msg);
                    if (strict) throw e;
                } catch (ExecutionException e) {
                    String msg = "Negotiating party " + agent_uuid + " threw an exception in init() method.";
                    if (logger != null) logger.info(msg);
                    if (strict) throw e;
                }
            } else {
                agent.init(info);
            }
        } else {
            AgentAdapter agent = this.agents.get(agent_uuid);
            NegotiationInfo info = createNegotiationInfo(domain_file_name, utility_file_name, real_time,
                    real_time ? (int) time_limit : (int) n_steps, 0, agent_uuid, agent_timeout, strict);
            if (info == null) {
                if (strict)
                    throw new InvalidObjectException(String.format("Cannot create NegotiationInfo for agent %s. Cannot start negotiation ", agent_uuid));
                return;
            }
            if (force_timeout_init) {
                ExecutorWithTimeout executor = executors.get(agent_uuid);
                try {
                    executor.execute(agent_uuid, () -> {
                        agent.init(info);
                        return agent;

                    });
                } catch (TimeoutException e) {
                    String msg = "Negotiating party " + agent_uuid + " timed out in init() method.";
                    if (logger != null) logger.info(msg);
                    if (strict) throw e;
                } catch (ExecutionException e) {
                    String msg = "Negotiating party " + agent_uuid + " threw an exception in init() method.";
                    if (logger != null) logger.info(msg);
                    if (strict) throw e;
                }
            } else {
                agent.init(info);
            }
        }
        n_total_negotiations++;
        n_active_negotiations++;
        if (is_debug) {
            info(String.format("Agent %s: time limit %d, step limit %d\n", getAgentName(agent_uuid), time_limit, n_steps));
        } else {
            printStatus();
        }
    }

    /**
     * Called by negmas to allow the agent to choose an action (e.g. reject, accept, end) and a counter offer (in case of reject)
     *
     * @param agent_uuid Agent UUID (must have already been created using create_agent(), and entered a negotaition with on_negotiation_start())
     * @param round      round number
     * @return A string serialization of the agent response.
     * @throws TimeoutException
     * @throws ExecutionException
     * @throws InvalidObjectException
     */
    public String choose_action(String agent_uuid, int round) throws TimeoutException, ExecutionException, InvalidObjectException {
        round++;
        n_total_responses++;
        Boolean isparty = is_party.get(agent_uuid);
        if (isparty == null) {
            if (is_debug) {
                String msg = String.format("Agent %s does not exist", agent_uuid);
                info(msg);
            }
            throw new InvalidObjectException(String.format("%s agent was not found. Cannot choose action (round %d)", agent_uuid, round));
        }
        if (isparty)
            return chooseActionParty(agent_uuid, round);
        return chooseActionAgent(agent_uuid, round);
    }

    /**
     * Called by negmas to tell the agent about an offer by another negotiator
     *
     * @param agent_uuid   Agent UUID (must already be created and initalized)
     * @param from_id      The agent that sent the action
     * @param typeOfAction The type of action chosen by the sender
     * @param bid_str      A string serialization of the bid by the sender
     * @param round        round number (negmas step)
     * @return Success/failure
     * @throws TimeoutException
     * @throws ExecutionException
     */
    public Boolean receive_message(String agent_uuid, String from_id, String typeOfAction, String bid_str, int round) throws TimeoutException, ExecutionException, InvalidObjectException {
        round++;
        n_total_offers++;
        boolean result;
        Boolean isparty = is_party.get(agent_uuid);
        if (isparty == null) {
            if (is_debug) {
                info(String.format("Agent %s does not exist", agent_uuid));
            }
            throw new InvalidObjectException(String.format("%s agent was not found. Cannot receive message (round %d)", agent_uuid, round));
        }
        if (isparty)
            result = receiveMessageParty(agent_uuid, from_id, typeOfAction, bid_str, round);
        else
            result = receiveMessageAgent(agent_uuid, from_id, typeOfAction, bid_str, round);
        return result;
    }

    /**
     * Informs the agent about the number of negotiators in the mechanism
     *
     * @param agent_uuid Agent UUID
     * @param agent_num  number of agents in the negotiation
     */
    public void inform_message(String agent_uuid, int agent_num) throws InvalidObjectException {
        if (ids.get(agent_uuid) == null) {
            if (is_debug) {
                info(String.format("Agent %s does not exist", agent_uuid));
            }
            throw new InvalidObjectException(String.format("%s agent was not found. Cannot inform message ", agent_uuid));
        }
        Inform inform = new Inform(ids.get(agent_uuid), "NumberOfAgents", agent_num);
        parties.get(agent_uuid).receiveMessage(ids.get(agent_uuid), inform);
    }

    /**
     * Informs the agent about the number of negotiators in the mechanism. Uses the number sent in create_agent
     *
     * @param agent_uuid Agent UUID
     */
    public void inform_message(String agent_uuid) {
        if (ids.get(agent_uuid) == null) {
            if (is_debug) {
                String msg = String.format("Agent %s does not exist", agent_uuid);
                info(msg);
            }
            return;
        }
        Inform inform = new Inform(ids.get(agent_uuid), "NumberOfAgents", this.n_agents);
        parties.get(agent_uuid).receiveMessage(ids.get(agent_uuid), inform);
    }

    /**
     * Called by negmas when the negotiation is ended to inform the Genius agent (does not cleanup)
     * @param agent_uuid Agent UUID
     * @param bid_str the agreement if any
     * @throws TimeoutException
     * @throws ExecutionException
     */
    public void on_negotiation_end(String agent_uuid, String bid_str) throws TimeoutException, ExecutionException, InvalidObjectException {
        Bid bid = bid_str == null ? null : strToBid(agent_uuid, bid_str);
        Boolean isparty = is_party.get(agent_uuid);
        boolean strict = is_strict.get(agent_uuid);
        boolean forcedTimeout = isRealTimeLimit.get(agent_uuid);
        if (isparty == null) {
            if (is_debug) {
                info(String.format("Agent %s does not exist", agent_uuid));
            }
            throw new InvalidObjectException(String.format("%s agent was not found. Cannot receive message", agent_uuid));
        }
        if (isparty) {
            AbstractNegotiationParty agent = parties.get(agent_uuid);
            if (forcedTimeout && force_timeout_end) {
                ExecutorWithTimeout executor = executors.get(agent_uuid);
                try {
                    executor.execute(agent.toString(), (Callable<Map<String, String>>) () -> agent.negotiationEnded(bid));
                } catch (TimeoutException e) {
                    String msg = "Negotiating party " + agent_uuid + " timed out in negotiationEnded() method.";
                    if (logger != null) logger.info(msg);
                    if (strict) throw e;
                } catch (ExecutionException e) {
                    String msg = "Negotiating party " + agent_uuid
                            + " threw an exception in negotiationEnded() method.";
                    if (logger != null) logger.info(msg);
                    if (strict) throw e;
                }
            } else {
                agent.negotiationEnded(bid);
                // todo: check to see if the original protocol in genius uses this return value
                // for anything
            }
        } else {
            AgentAdapter agent = agents.get(agent_uuid);
            if (forcedTimeout && force_timeout_end) {
                ExecutorWithTimeout executor = executors.get(agent_uuid);
                try {
                    executor.execute(agent.toString(), () -> agent.negotiationEnded(bid));
                } catch (TimeoutException e) {
                    String msg = "Negotiating party " + agent_uuid + " timed out in negotiationEnded() method.";
                    if (logger != null) logger.info(msg);
                    if (strict) throw e;
                } catch (ExecutionException e) {
                    String msg = "Negotiating party " + agent_uuid
                            + " threw an exception in negotiationEnded() method.";
                    if (logger != null) logger.info(msg);
                    if (strict) throw e;
                }
            } else {
                agent.negotiationEnded(bid);
            }
        }
    }

    /**
     * Destroys an agent (should only be called after the negotiation is ended)
     * @param agent_uuid Agent UUID
     * @return Always an empty string
     */
    public String destroy_agent(String agent_uuid) {
        this.is_strict.remove(agent_uuid);
        this.isRealTimeLimit.remove(agent_uuid);
        this.ids.remove(agent_uuid);
        this.util_spaces.remove(agent_uuid);
        this.first_actions.remove(agent_uuid);
        if (force_any_timeout)
            this.executors.remove(agent_uuid);
        this.string2values.remove(agent_uuid);
        this.string2issues.remove(agent_uuid);
        this.parties.remove(agent_uuid);
        this.agents.remove(agent_uuid);
        this.is_party.remove(agent_uuid);
        n_active_agents--;
        if (is_debug) {
            String msg = String.format("Agent %s destroyed\n", agent_uuid);
            info(msg);
        } else {
            printStatus();
        }
        System.gc();
        return "";
    }

    /**
     * Clears all agents and data-structures
     */
    public void clean() {
        this.is_strict.clear();
        this.isRealTimeLimit.clear();
        this.ids.clear();
        this.util_spaces.clear();
        this.first_actions.clear();
        if (force_any_timeout)
            this.executors.clear();
        this.string2values.clear();
        this.string2issues.clear();
        this.parties.clear();
        this.agents.clear();
        this.is_party.clear();
        n_active_agents = this.ids.size();
        System.gc();
        if (is_debug) {
            String msg = "All agents destroyed";

            info(msg);

        } else {
            printStatus();
        }
    }

    /**
     * Shuts down the Py4J server
     */
    public void shutdown() {
        info("Shutting down Py4j");
        this.shutdownPy4jServer();
    }

    /**
     * Tries to interrupt all threads (except the main), waits for the wait_time in
     * milliseconds then kills them all.
     */
    public void kill_threads(int wait_time) {
        info("Kill threads was sent to this bridge");
        Thread current = Thread.currentThread();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (current != t && t.getState() == Thread.State.RUNNABLE)
                t.interrupt();
        }

        try {
            Thread.sleep(wait_time);
        } catch (InterruptedException e) {
            String msg = "Main thread interrupted!!";

            info(msg);
            current.interrupt();
            return;
        }
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (current != t && t.getState() == Thread.State.RUNNABLE)
                t.stop();
        }
    }

    /**
     * Kills the bridge.
     */
    public void kill() {
        info("Kill was sent to this bridge");
        this.shutdown();
        System.exit(0);
    }


    /**
     * Runs a negotiation inside Genius
     * @param p The protocol
     * @param domainFile Domain xml file
     * @param sProfiles Semicolon separated profile file paths
     * @param sAgents Semiconon separated agent class names
     * @param outputFile File used for logging
     * @return succcess/failure
     * @throws Exception
     */
    public boolean run_negotiation(String p, String domainFile, String sProfiles, String sAgents, String outputFile) throws Exception {
        List<String> agents = Arrays.asList(sAgents.split(";", -1));
        List<String> profiles = Arrays.asList(sProfiles.split(";", -1));
        return this._run_negotiation(p, domainFile, profiles, agents, outputFile);
    }

    /**
     * Runs a negotiation inside Genius
     * @param p The protocol
     * @param domainFile Domain xml file
     * @param profiles list of profile files
     * @param agents list of agent class names
     * @param outputFile File used for logging
     * @return succcess/failure
     * @throws Exception
     */
    private boolean _run_negotiation(String p, String domainFile, List<String> profiles, List<String> agents, String outputFile) throws Exception {
        if (!is_silent)
            print_negotiation_info(outputFile, agents, profiles, domainFile, p);
        if (p == null || domainFile == null) {
            return false;
        }
        if (profiles.size() != agents.size())
            return false;

        Global.logPreset = outputFile;
        Protocol ns;

        ProtocolRepItem protocol = new ProtocolRepItem(p, p, p);

        DomainRepItem dom = new DomainRepItem(new URL(domainFile));

        ProfileRepItem[] agentProfiles = new ProfileRepItem[profiles.size()];
        for (int i = 0; i < profiles.size(); i++) {
            agentProfiles[i] = new ProfileRepItem(new URL(profiles.get(i)), dom);
            if (!is_silent)
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
            if (!is_silent)
                System.out.format("Agent Type: %s\n", agentsrep[i].toString());
        }

        ns = Global.createProtocolInstance(protocol, agentsrep, agentProfiles, agentParams);

        ns.startSession();
        ns.wait();
        return true;
    }

    /* --------------------------------------------------- */
    /// Negotiation Callbacks used by Python-hook methods.
    /// these are the only methods that call the Genius Agent.
    /* --------------------------------------------------- */

    /**
     * Chooses an action by the agent (for old agents based on the Agent class)
     * @param agent_uuid Agent UUID (must already been created by create_agent and initalized)
     * @param round round number (negmas step)
     * @return A string serialization of the action chosen (including counter offer if any)
     * @throws ExecutionException
     * @throws TimeoutException
     * @throws InvalidObjectException
     */
    private String chooseActionAgent(String agent_uuid, int round) throws ExecutionException, TimeoutException, InvalidObjectException {
        AgentAdapter agent = agents.get(agent_uuid);
        Boolean isFirstTurn = first_actions.get(agent_uuid);
        boolean strict = is_strict.get(agent_uuid);
        boolean forcedTimeout = isRealTimeLimit.get(agent_uuid);
        TimeLineInfo timeline = ((Agent) agent).timeline;
        if (is_debug) {
            info(String.format("\tRelative time for %s is %f [round %d]\n", agent_uuid, timeline.getTime(), round));
        }
        List<Class<? extends Action>> validActions = new ArrayList<>();
        if (isFirstTurn == null) {
            isFirstTurn = true;
        }
        if (!isFirstTurn)
            validActions.add(Accept.class);
        validActions.add(EndNegotiation.class);
        validActions.add(Offer.class);
        genius.core.actions.Action action = null;
        if (forcedTimeout && force_timeout) {
            ExecutorWithTimeout executor = getExecutor(timeline);
            try {
                action = executor.execute(agent.toString(), () -> agent.chooseAction(validActions));
            } catch (TimeoutException e) {
                String msg = "Negotiating party " + agent_uuid + " timed out in chooseAction() method.";
                if (logger != null) logger.info(msg);
                if (strict) throw e;
            } catch (ExecutionException e) {
                String msg = "Negotiating party " + agent_uuid + " threw an exception in chooseAction() method.";
                if (logger != null) logger.info(msg);
                if (strict) throw e;
            }
        } else {
            action = agent.chooseAction(validActions);
        }
        if (action == null) {
            if (is_debug && !is_silent) {
                System.out.format("\t%s NO ACTION is received", agent_uuid);
            }
            if (strict)
                throw new InvalidObjectException(String.format("Agent %s responded to chooseAction with null", agent_uuid));
            return getAgentName(agent_uuid) + FIELD_SEP + "NoAction" + FIELD_SEP;
        }
        if (is_debug) {
            info(String.format("\t%s -> %s\n", agent_uuid, action.toString()));
        } else {
            printStatus();
        }
        if (timeline instanceof DiscreteTimeline) {
            if (round < 0)
                ((DiscreteTimeline) timeline).increment();
            else
                ((DiscreteTimeline) timeline).setcRound(round);
        }
        return actionToString(action);
    }


    /**
     * Chooses an action by the agent (for new agents based on the NegotiationParty class)
     * @param agent_uuid Agent UUID (must already been created by create_agent and initalized)
     * @param round round number (negmas step)
     * @return A string serialization of the action chosen (including counter offer if any)
     * @throws ExecutionException
     * @throws TimeoutException
     * @throws InvalidObjectException
     */
    private String chooseActionParty(String agent_uuid, int round) throws TimeoutException, ExecutionException, InvalidObjectException {
        AbstractNegotiationParty agent = parties.get(agent_uuid);
        Boolean isFirstTurn = first_actions.get(agent_uuid);
        boolean strict = is_strict.get(agent_uuid);
        boolean forcedTimeout = isRealTimeLimit.get(agent_uuid);
        TimeLineInfo timeline = agent.getTimeLine();
        if (is_debug) {
            String msg = String.format("\tRelative time for %s is %f\n", agent_uuid, timeline.getTime());
            info(msg);

        }
        List<Class<? extends Action>> validActions = new ArrayList<>();
        if (isFirstTurn == null) {
            isFirstTurn = true;
        }
        if (!isFirstTurn)
            validActions.add(Accept.class);
        validActions.add(EndNegotiation.class);
        validActions.add(Offer.class);
        genius.core.actions.Action action = null;
        if (forcedTimeout && force_timeout) {
            ExecutorWithTimeout executor = getExecutor(timeline);
            try {
                action = executor.execute(agent.toString(), () -> agent.chooseAction(validActions));
            } catch (TimeoutException e) {
                String msg = "Negotiating party " + agent_uuid + " timed out in chooseAction() method.";
                if (logger != null) logger.info(msg);
                if (strict) throw e;
            } catch (ExecutionException e) {
                String msg = "Negotiating party " + agent_uuid + " threw an exception in chooseAction() method.";
                if (logger != null) logger.info(msg);
                if (strict) throw e;
            }
        } else {
            action = agent.chooseAction(validActions);
        }
        if (action == null) {
            if (is_debug && !is_silent) {
                System.out.format("\t%s NO ACTION is received", agent_uuid);
            }
            if (strict)
                throw new InvalidObjectException(String.format("Agent %s responded to chooseAction with null", agent_uuid));
            return getAgentName(agent_uuid) + FIELD_SEP + "NoAction" + FIELD_SEP;
        }
        if (is_debug) {
            info(String.format("\t%s -> %s\n", agent_uuid, action.toString()));
        } else {
            printStatus();
        }
        if (timeline instanceof DiscreteTimeline)
            if (round < 0)
                ((DiscreteTimeline) timeline).increment();
            else
                ((DiscreteTimeline) timeline).setcRound(round);
        return actionToString(action);
    }

    /**
     * Sends a message to an old agent based on the Agent class
     * @param agent_uuid Agent UUID (must be created using create_agent and initalized)
     * @param from_id sender id
     * @param typeOfAction Sender's action
     * @param bid_str string serialization of the sender's bid
     * @param round round number (negmas step)
     * @return success/failure
     * @throws ExecutionException
     * @throws TimeoutException
     * @throws InvalidObjectException
     */
    private Boolean receiveMessageAgent(String agent_uuid, String from_id, String typeOfAction, String bid_str, int round) throws ExecutionException, TimeoutException, InvalidObjectException {
        AgentAdapter agent = agents.get(agent_uuid);
        Boolean isFirstTurn = first_actions.get(agent_uuid);
        boolean strict = is_strict.get(agent_uuid);
        boolean forcedTimeout = isRealTimeLimit.get(agent_uuid);
        if (isFirstTurn == null){
            isFirstTurn = true;
        }
        if (isFirstTurn)
            first_actions.put(agent_uuid, false);
        Bid bid = strToBid(agent_uuid, bid_str);
        if (bid == null)
            throw new InvalidObjectException(String.format("Agent %s received NULL bid  %s", agent_uuid, bid_str));
        AgentID agentID = new AgentID(from_id);
        printStatus();
        TimeLineInfo timeline = ((Agent) agent).timeline;
        if (is_debug) {
            String msg = String.format("\tRelative time for %s (receive) is %f\n", agent_uuid, timeline.getTime());
            info(msg);
        }
        if (timeline instanceof DiscreteTimeline) {
            if (round >= 0)
                ((DiscreteTimeline) timeline).setcRound(round);
        }
        final Action act = typeOfAction.contains("Offer") ? new Offer(agentID, bid)
                : typeOfAction.contains("Accept") ? new Accept(agentID, bid)
                : typeOfAction.contains("EndNegotiation") ? new EndNegotiation(agentID) : null;
        // agent.receiveMessage(agentID, act);
        if (forcedTimeout && force_timeout) {
            ExecutorWithTimeout executor = getExecutor(timeline);
            try {
                executor.execute(agent.toString(), () -> {
                    agent.receiveMessage(agentID, act);
                    return agentID;
                });
            } catch (TimeoutException e) {
                String msg = "Negotiating party " + agent_uuid + " timed out in receiveMessage() method.";
                if (logger != null) logger.info(msg);
                if (strict) throw e;
            } catch (ExecutionException e) {
                String msg = "Negotiating party " + agent_uuid + " threw an exception in receiveMessage() method.";
                if (logger != null) logger.info(msg);
                if (strict) throw e;
            }
        } else {
            agent.receiveMessage(agentID, act);
        }
        if (is_debug && !is_silent) {
            System.out.flush();
        }
        return true;
    }

    /**
     * Sends a message to an new agent based on the NegotiationParty class
     * @param agent_uuid Agent UUID (must be created using create_agent and intialized)
     * @param from_id sender id
     * @param typeOfAction Sender's action
     * @param bid_str string serialization of the sender's bid
     * @param round round number (negmas step)
     * @return success/failure
     * @throws ExecutionException
     * @throws TimeoutException
     * @throws InvalidObjectException
     */
    private Boolean receiveMessageParty(String agent_uuid, String from_id, String typeOfAction, String bid_str, int round) throws TimeoutException, ExecutionException, InvalidObjectException {
        AbstractNegotiationParty agent = parties.get(agent_uuid);
        Boolean isFirstTurn = first_actions.get(agent_uuid);
        boolean strict = is_strict.get(agent_uuid);
        boolean forcedTimeout = isRealTimeLimit.get(agent_uuid);
        if (isFirstTurn == null){
            isFirstTurn = true;
        }
        if (isFirstTurn)
            first_actions.put(agent_uuid, false);
        Bid bid = strToBid(agent_uuid, bid_str);
        if (bid == null)
            throw new InvalidObjectException(String.format("Agent %s received NULL bid  %s", agent_uuid, bid_str));
        AgentID agentID = new AgentID(from_id);
        printStatus();
        TimeLineInfo timeline = agent.getTimeLine();
        if (is_debug) {
            info(String.format("\tRelative time for %s (receive) is %f [round %d]\n", agent_uuid, timeline.getTime(), round));
        }
        if (timeline instanceof DiscreteTimeline) {
            if (round >= 0)
                ((DiscreteTimeline) timeline).setcRound(round);
        }
        final Action act = typeOfAction.contains("Offer") ? new Offer(agentID, bid)
                : typeOfAction.contains("Accept") ? new Accept(agentID, bid)
                : typeOfAction.contains("EndNegotiation") ? new EndNegotiation(agentID) : null;
        if (forcedTimeout && force_timeout) {
            ExecutorWithTimeout executor = getExecutor(timeline);
            try {
                executor.execute(agent.toString(), () -> {
                    agent.receiveMessage(agentID, act);
                    return agentID;
                });
            } catch (TimeoutException e) {
                String msg = "Negotiating party " + agent_uuid + " timed out in receiveMessage() method.";
                if (logger != null) logger.info(msg);
                if (strict) throw e;
            } catch (ExecutionException e) {
                String msg = "Negotiating party " + agent_uuid + " threw an exception in receiveMessage() method.";
                if (logger != null) logger.info(msg);
                if (strict) throw e;
            }
        } else {
            agent.receiveMessage(agentID, act);
        }
        return true;
    }

    /// -------
    /// Helpers
    /// -------

    /**
     * Creates an executor with the remaining time of the negotiation
     * @param timeline The timeline
     * @return An executor
     */
    private ExecutorWithTimeout getExecutor(TimeLineInfo timeline) {
        double timeout = timeline.getTotalTime() - timeline.getCurrentTime();
        timeout = (timeout < 0) ? 0 : timeout;
        return new ExecutorWithTimeout((long) timeout);
    }

    private HashMap<String, HashMap<String, Value>> initStrValConversion(ArrayList<Issue> issues) {
        HashMap<String, HashMap<String, Value>> string2value = new HashMap<>();
        for (Issue issue : issues) {
            String issue_name = issue.toString();
            string2value.put(issue_name, new HashMap<>());
            List<ValueDiscrete> values = ((IssueDiscrete) issue).getValues();
            for (Value value : values) {
                string2value.get(issue_name).put(value.toString(), value);
            }
        }
        return string2value;
    }

    /**
     * Creates a NegotiationInfo structure to pass to the Genius agent to initalize it
     *
     * @param domain_file_name   Domain xml file
     * @param utility_file_name  Preferences xml file
     * @param real_time          If true, a ContinuousTimeline is used otherwise a DiscreteTimeline is used
     * @param max_time           time_limit or n_steps depending on real_time
     * @param seed               A random seed
     * @param agent_uuid         Agent UUID (must have already been created using create_agent())
     * @param max_time_per_agent Maximum allowed time per agent
     * @param strict             If given all exceptions and failures to choose an action are passed to negmas
     * @return
     */
    private NegotiationInfo createNegotiationInfo(String domain_file_name, String utility_file_name, boolean real_time,
                                                  int max_time, long seed, String agent_uuid, long max_time_per_agent, boolean strict) {
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
            AgentID agentID = new AgentID(agent_uuid);
            DefaultPersistentDataContainer storage = new DefaultPersistentDataContainer(new Serialize(),
                    PersistentDataType.DISABLED);
            NegotiationInfo info = new NegotiationInfo(utilSpace, deadline, timeline, seed, agentID, storage);
            first_actions.put(agent_uuid, true);
            is_strict.put(agent_uuid, strict);
            isRealTimeLimit.put(agent_uuid, strict);
            ids.put(agent_uuid, agentID);
            util_spaces.put(agent_uuid, utilSpace);
            isRealTimeLimit.put(agent_uuid, real_time);
            if (force_any_timeout)
                executors.put(agent_uuid, new ExecutorWithTimeout(timeout));
            ArrayList<Issue> issues = (ArrayList<Issue>) utilSpace.getDomain().getIssues();
            string2values.put(agent_uuid, this.initStrValConversion(issues));
            HashMap<String, Issue> striss = new HashMap<>();
            for (Issue issue : issues) {
                striss.put(issue.toString(), issue);
            }
            string2issues.put(agent_uuid, striss);
            return info;
        } catch (Exception e) {
            // TODO: handle exception
            if (!is_silent)
                System.out.println(e.toString());
        }
        return null;
    }

    private String getAgentName(String agent_uuid) {
        try {
            if (this.is_party.get(agent_uuid))
                return parties.get(agent_uuid).getDescription();
            else
                return agents.get(agent_uuid).getDescription();
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }

    private String actionToString(Action action) {
        // System.out.print("Entering action to string");
        String id = action.getAgent().getName();
        Bid bid;
        if (action instanceof Offer) {
            bid = ((Offer) action).getBid();
            if (bid == null) {
                // Here we assume that offering None is handled the same way
                // it is done in negmas. It may lead to ending the negotiation
                // or just a new offer
                return id + FIELD_SEP + "NullOffer" + FIELD_SEP;
            }
            List<Issue> issues = bid.getIssues();
            HashMap<String, String> vals_str = new HashMap<>();
            for (Issue issue : issues) {
                Value value = bid.getValue(issue.getNumber());
                vals_str.put(issue.toString(), value.toString());
            }

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

        if (bid_str == null) {
            String msg = String.format("Received null bid ID %s", agent_uuid);
            info(msg);
            return null;
        }
        if (bid_str.equals("")) {
            String msg = String.format("Received empty bid ID %s", agent_uuid);
            info(msg);
            return null;
        }
        String[] bid_strs = bid_str.split(ENTRY_SEP);
        HashMap<Integer, Value> vals = new HashMap<>();
        for (String str : bid_strs) {
            String[] vs = str.split(INTERNAL_SEP);
            String issue_name = vs[0];
            String val = vs.length > 1 ? vs[1] : "";
            vals.put(string2issues.get(agent_uuid).get(issue_name).getNumber(),
                    string2values.get(agent_uuid).get(issue_name).get(val));
        }
        Bid bid;
        try {
            bid = new Bid(utilSpace.getDomain(), vals);
        } catch (Exception e) {
            bid = null;
            warning(e.toString());
        }
        return bid;
    }

    private void printStatus() {
        if (is_silent)
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

    private void printException(Exception e) {
        if (!is_silent)
            e.printStackTrace();
    }

    /// ----------------------
    /// Py4J server Management
    /// ----------------------
    public void setPy4jServer(GatewayServer server) {
        info("Py4j Server is set");
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
        String msg = String.format("Gateway %s to python started at port %d listening to port %d [%s: %d]\n", version(), port,
                listening_port, force_timeout ? "forcing timeout" : "no  timeout", this.global_timeout);
        if (!is_silent)
            System.out.print(msg);
        info(msg);
    }

    public void shutdownPy4jServer() {
        server.shutdown();
    }

    public static class Serialize implements Serializable {
    }
}
