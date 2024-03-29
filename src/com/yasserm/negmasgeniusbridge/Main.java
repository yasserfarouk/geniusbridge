/*
 * A bridge that connects NegMAS to Genius.
 *
 * Notes:
 * =====
 * - A step in NegMAS == round in Genius
 * - A negotiator in NegMAS == agent in Genius
 * - Agents in Genius come in two varieties: older agents are based on the Agent class and newer ones on the NegotiationParty class.  We support both
 * - An AgentAdapter adapts an Agent class to appear as a NegotiationParty class (to the best of my knowledge).
 * - It seems that rounds start at 1 in Genius but steps start at 0 in NegMAS. For this reason, rounds/steps exchanged are adjusted down/up by one
 *
 * Unclear Ponints:
 * ================
 * - How to run a negotiation programatically in Genius?
 * - How to run a single round of a negotiation programatically in Genius?
 *
 */
package com.yasserm.negmasgeniusbridge;

import genius.core.*;
import genius.core.actions.*;
import genius.core.exceptions.NegotiatorException;
import genius.core.issue.*;
import genius.core.list.Tuple;
import genius.core.logging.ConsoleLogger;
import genius.core.logging.FileLogger;
import genius.core.logging.XmlLogger;
import genius.core.parties.*;
import genius.core.persistent.DefaultPersistentDataContainer;
import genius.core.persistent.PersistentDataType;
import genius.core.protocol.MultilateralProtocol;
import genius.core.protocol.Protocol;
import genius.core.repository.*;
import genius.core.session.*;
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
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import java.util.logging.*;

/** Prevents printing */
class NullPrinter extends PrintStream
{
    public NullPrinter(OutputStream out)
    {
        super(out, true);
    }
    @Override
    public void print(String s) {}
    @Override
    public void println(String s) {}
}

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
}

/**
 * Representas a running Genius Agent
 */
class NegotiatorInfo {
    final public NegotiationParty agent;
    public AgentID id;
    public Boolean isStrict;
    public Boolean isRealTimeLimit;
    public AdditiveUtilitySpace utilitySpace;
//    public HashMap<String, HashMap<String, Value>> string2values = new HashMap<>();
    public HashMap<String, Issue> string2issues = new HashMap<>();
    public Boolean firstAction = true;
    public ExecutorWithTimeout executor;
    public long timeout;

    public NegotiatorInfo(NegotiationParty agent) {
        this.agent = agent;
    }
}

class NegLoader {
    final private HashMap<String, NegotiatorInfo> agents = new HashMap<>();
    final private String INTERNAL_SEP = "<<s=s>>";
    final private String ENTRY_SEP = "<<y,y>>";
    final private String FIELD_SEP = "<<sy>>";
    final private String TIMEOUT = "__TIMEOUT__";
    final private String OK = "__OK__";
    final private String FAILED = "__FAILED__";
    final private String NOACTION = "NoAction";
    final public boolean isDebug;
    final private boolean forceTimeout;
    final private boolean forceAnyTimeout;
    final private boolean forceTimeoutInInit;
    final private boolean forceTimeoutInEnd;
    final private Logger logger;
    final private boolean logging;
    final private boolean agentPrint;
    final private boolean isSilent;
    private long nTotalAgents = 0;
    private long nActiveAgents = 0;
    private long nTotalOffers = 0;
    private long nTotalResponses = 0;
    private GatewayServer server;
    private int nAgents = 0;
    private long globalTimeoutMs = 180000;

    public NegLoader() {
        this(false, false, false, false, 0, true, null, true, true);
    }

    /**
     * @param is_debug           Enable debug mode with more printing and logging
     * @param forceTimeout       If given, timeouting will be enforced. Note that this is only effective for agents that are created with a ContinuousTimeline (i.e. finite negmas time_limit)
     * @param forceTimeoutInInit If given, timeouting is enforced in `on_negotiation_start`
     * @param forceTimeoutInEnd  If given, timeouting is enforced in `on_negotaition_end`
     * @param timeout            The timeout in seconds
     * @param logging            If True, logging is enabled
     * @param logger             If given and logging==true, the logger to use
     * @param isSilent           If given, no printing to the screen is allowed
     * @param agentPrint         If true, agents are allowed to print
     */
    public NegLoader(boolean is_debug, boolean forceTimeout, boolean forceTimeoutInInit, boolean forceTimeoutInEnd,
                     long timeout, boolean logging, Logger logger, boolean isSilent, boolean agentPrint) {
        this.logger = logger;
        this.logging = logging;
        this.isDebug = is_debug;
        this.isSilent = isSilent;
        this.agentPrint = agentPrint;
        this.forceTimeoutInInit = forceTimeoutInInit;
        this.forceTimeoutInEnd = forceTimeoutInEnd;
        if (timeout > 0)
            this.globalTimeoutMs = timeout;
        this.forceTimeout = forceTimeout;
        this.forceAnyTimeout = forceTimeout || forceTimeoutInEnd || forceTimeoutInInit;
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
        boolean force_timeout_end = true;
        boolean logging = false;
        boolean agent_print = false;
        var logFile = "genius-bridge-log.txt";
        var s = new StringBuilder("received options: ");
        boolean run_neg = false;
        List<String> agents = new ArrayList<>();
        List<String> profiles = new ArrayList<>();
        String domainFile = null, protocol = null, outputFile = null;
        int nSteps = -1, timeLimit = -1;
        for (String opt : args) {
            s.append(String.format("%s ", opt));
            if (opt.startsWith("--version") || opt.startsWith("version")) {
                System.out.println(version());
                System.exit(0);
            }
            if (run_neg) {
                if (opt.startsWith("-u") || opt.startsWith("--profile") || opt.startsWith("--ufun")) {
                    profiles.add(String.format("%s", opt.split("=")[1]));
                }
                if (opt.startsWith("-a") || opt.startsWith("--agent") || opt.startsWith("-n") || opt.startsWith("--negotiator")) {
                    agents.add(String.format("%s", opt.split("=")[1]));
                }
                if (opt.startsWith("-p") || opt.startsWith("--protocol") || opt.startsWith("--mechanism")) {
                    protocol = opt.split("=")[1];
                }
                if (opt.startsWith("-d") || opt.startsWith("--domain") || opt.startsWith("--issues")) {
                    domainFile = String.format("%s", opt.split("=")[1]);
                }
                if (opt.startsWith("-o") || opt.startsWith("--log") || opt.startsWith("--output")) {
                    outputFile = String.format("%s", opt.split("=")[1]);
                }
                if (opt.startsWith("-n") || opt.startsWith("--n-steps") || opt.startsWith("--steps")) {
                    nSteps = Integer.parseInt(opt.split("=")[1]);
                }
                if (opt.startsWith("-t") || opt.startsWith("--time") || opt.startsWith("--timeLimit")) {
                    timeLimit = Integer.parseInt(opt.split("=")[1]);
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
            } else if (opt.startsWith("--allow-agent-print") || opt.equals("allow-agent-print")) {
                agent_print = true;
            } else {
                port = Integer.parseInt(opt);
            }
        }
        if (run_neg) {
            if (nSteps >= 0 && timeLimit >= 0) {
                System.out.format("You are specifying %d steps and %d time-limit. Cannot specify both", nSteps, timeLimit);
                System.exit(-1);
            }
            if (nSteps < 0 && timeLimit < 0) {
                System.out.format("You are specifying %d steps and %d time-limit. Must specify one of them", nSteps, timeLimit);
                System.exit(-1);
            }
            var n = new NegLoader();
            boolean result = false;
            try {
                result = n.runNegotiation(protocol, domainFile, profiles, agents, outputFile, nSteps, timeLimit);
            } catch (Exception e) {
                if (!is_silent)
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
                logging ? logger : null, is_silent, agent_print);
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
        return "v0.2.4";
    }

    /// Python Hooks: Methods called from python (they all have python snake_case
    /// naming convention)

    /**
     * Creates a new agent (negmas Negotaitor)
     *
     * @param class_name The class name of the negotiator to create
     * @return If successful the UUID of the object created otherwise an FAILED
     * @throws InstantiationException    Failed to create the agent
     * @throws IllegalAccessException    Failed to create the agent
     * @throws ClassNotFoundException    Failed to create the agent
     * @throws InvocationTargetException Failed to create the agent
     * @throws NoSuchMethodException     Failed to create the agent
     */
    public String create_agent(String class_name) throws InstantiationException, IllegalAccessException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException {
        try {
            Class<?> clazz = Class.forName(class_name);
            String uuid = class_name + UUID.randomUUID().toString();
            NegotiationParty agent;
            if (AbstractNegotiationParty.class.isAssignableFrom(clazz)) {
                agent = (AbstractNegotiationParty) clazz.getDeclaredConstructor().newInstance();
            } else {
                agent = (AgentAdapter) clazz.getDeclaredConstructor().newInstance();
            }
            this.agents.put(uuid, new NegotiatorInfo(agent));
            nTotalAgents++;
            nActiveAgents++;

            if (isDebug) {
                info(String.format("Creating Agent of type %s (ID= %s)\n", class_name, uuid));
            } else {
                this.printStatus();
            }
            return uuid;
        } catch (Exception e) {
            printException(e);
            throw e;
        }
    }

    /**
     * Gets the relative time as seen by the Genius agent
     *
     * @param agent_uuid Agent UUID
     * @return relative time [0-1] of the negotiation (works both for n_steps and time_limit limited negotiations)
     */
    public double get_relative_time(String agent_uuid) {
        NegotiationParty agent = agents.get(agent_uuid).agent;
        TimeLineInfo timeline = getTimeline(agent);
        return timeline.getTime();
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
     * @return OK if success, FAILED if failure, TIMEOUT if timedout
     * @throws ExecutionException The Genius agent threw an exception
     * @throws InvalidObjectException Cannot find the genius agent
     */
    public String on_negotiation_start(String agent_uuid, int n_agents, long n_steps, long time_limit, boolean real_time,
                                       String domain_file_name, String utility_file_name, long agent_timeout, boolean strict) throws ExecutionException, InvalidObjectException {
        this.nAgents = n_agents;
        if (isDebug) {
            info(String.format("Domain file: %s\nUfun: %s\nAgent: %s", domain_file_name, utility_file_name, agent_uuid));
        }
        NegotiatorInfo ainfo = this.agents.get(agent_uuid);
        NegotiationParty agent = ainfo.agent;
        TimeLineInfo timeline = getTimeline(agent);
        int seed = isDebug ? ThreadLocalRandom.current().nextInt(0, 10000) : 0;
        NegotiationInfo info_ = null;

        try{
            info_ = createNegotiationInfo(domain_file_name, utility_file_name, real_time,
                    real_time ? (int) time_limit : (int) n_steps, seed, agent_uuid, agent_timeout, strict);
          }
        catch(IOException e){
            if (strict)
                throw new InvalidObjectException(String.format("Cannot create NegotiationInfo for agent %s. Raised %s. Cannot start negotiation ", agent_uuid, e.toString()));
            return FAILED;
        }
        final NegotiationInfo info = info_;
        if (forceTimeoutInInit) {
            ExecutorWithTimeout executor = getExecutor(timeline, ainfo.timeout);
            try {
                executor.execute(agent_uuid, () -> {
                    List<PrintStream> l = this.ignoreOutput();
                    agent.init(info);
                    this.restoreOutput(l);
                    return agent;
                });
            } catch (TimeoutException e) {
                if (isDebug)
                    info("Negotiating party " + agent_uuid + " timed out in init() method.");
                return TIMEOUT;
            } catch (ExecutionException e) {
                error("Negotiating party " + agent_uuid + " threw an exception in init() method.");
                printException(e);
                if (strict) throw e;
                return FAILED;
            }
        } else {
            List<PrintStream> lst = this.ignoreOutput();
            agent.init(info);
            this.restoreOutput(lst);
        }
        if (isDebug) {
            info(String.format("Agent %s: time limit %d, step limit %d\n", getName(agent_uuid), time_limit, n_steps));
        } else {
            printStatus();
        }
        return OK;
    }

    /**
     * Called by negmas to allow the agent to choose an action (e.g. reject, accept, end) and a counter offer (in case of reject)
     *
     * @param agent_uuid Agent UUID (must have already been created using create_agent(), and entered a negotaition with on_negotiation_start())
     * @param round      round number
     * @return A string serialization of the agent response.
     * @throws ExecutionException     Genius agent did not respond
     * @throws InvalidObjectException Genius agent responded with a NULL action
     */
    public String choose_action(String agent_uuid, int round) throws ExecutionException, InvalidObjectException {
        round++;
        nTotalResponses++;
        NegotiatorInfo ainfo = agents.get(agent_uuid);
        NegotiationParty agent = ainfo.agent;
        Boolean isFirstTurn = ainfo.firstAction;
        boolean strict = ainfo.isStrict;
        boolean forcedTimeout = ainfo.isRealTimeLimit;
        TimeLineInfo timeline = getTimeline(agent);
        if (isDebug) {
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
        genius.core.actions.Action action;
        List<PrintStream> lst = this.ignoreOutput();
        if (forcedTimeout && forceTimeout) {
            ExecutorWithTimeout executor = getExecutor(timeline, ainfo.timeout);
            try {
                action = executor.execute(agent.toString(), () -> agent.chooseAction(validActions));
            } catch (TimeoutException e) {
                if (isDebug)
                    info("Negotiating party " + agent_uuid + " timed out in chooseAction() method.");
                return getName(agent_uuid) + FIELD_SEP + TIMEOUT + FIELD_SEP;
            } catch (ExecutionException e) {
                error("Negotiating party " + agent_uuid + " threw an exception in chooseAction() method.");
                printException(e);
                if (strict) throw e;
                return getName(agent_uuid) + FIELD_SEP + FAILED + FIELD_SEP;
            }
        } else {
            action = agent.chooseAction(validActions);
        }
        this.restoreOutput(lst);

        if (action == null) {
            error(String.format("\t%s NO ACTION is received", agent_uuid));
            if (strict)
                throw new InvalidObjectException(String.format("Agent %s responded to chooseAction with null", agent_uuid));
            return getName(agent_uuid) + FIELD_SEP + NOACTION + FIELD_SEP;
        }
        if (isDebug) {
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
     * Called by negmas to tell the agent about an offer by another negotiator
     *
     * @param agent_uuid   Agent UUID (must already be created and initalized)
     * @param from_id      The agent that sent the action
     * @param typeOfAction The type of action chosen by the sender
     * @param bid_str      A string serialization of the bid by the sender
     * @param round        round number (negmas step)
     * @return OK if success, FAILED if failure, TIMEOUT if timedout
     * @throws ExecutionException     Genius agent threw an exception
     * @throws InvalidObjectException Genius agent canot be found
     */
    public String receive_message(String agent_uuid, String from_id, String typeOfAction, String bid_str, int round) throws ExecutionException, InvalidObjectException {
        round++;    // Rounds in Genius start at 1 not 0!!!!
        nTotalOffers++;
        NegotiatorInfo ainfo = agents.get(agent_uuid);
        NegotiationParty agent = ainfo.agent;
        Boolean isFirstTurn = ainfo.firstAction;
        boolean strict = ainfo.isStrict;
        boolean forcedTimeout = ainfo.isRealTimeLimit;
        if (isFirstTurn)
            ainfo.firstAction = false;
        Bid bid = strToBid(agent_uuid, bid_str);
        if (bid == null)
            throw new InvalidObjectException(String.format("Agent %s received NULL bid  %s", agent_uuid, bid_str));
        AgentID agentID = new AgentID(from_id);
        printStatus();
        TimeLineInfo timeline = getTimeline(agent);
        if (isDebug) {
            info(String.format("\tRelative time for %s (receive) is %f [round %d]\n", agent_uuid, timeline.getTime(), round));
        }
        if (timeline instanceof DiscreteTimeline && round >= 0) {
            ((DiscreteTimeline) timeline).setcRound(round);
        }
        final Action act = getAction(typeOfAction, bid, agentID);
        List<PrintStream> lst = this.ignoreOutput();
        if (forcedTimeout && forceTimeout) {
            ExecutorWithTimeout executor = getExecutor(timeline, ainfo.timeout);
            try {
                executor.execute(agent.toString(), () -> {
                    agent.receiveMessage(agentID, act);
                    return agentID;
                });
            } catch (TimeoutException e) {
                if (isDebug)
                    info("Negotiating party " + agent_uuid + " timed out in receiveMessage() method.");
                return TIMEOUT;
            } catch (ExecutionException e) {
                error("Negotiating party " + agent_uuid + " threw an exception in receiveMessage() method.");
                printException(e);
                if (strict) throw e;
                return FAILED;
            }
        } else {
            agent.receiveMessage(agentID, act);
        }
        this.restoreOutput(lst);
        return OK;
    }

    /**
     * Informs the agent about the number of negotiators in the mechanism
     *
     * @param agent_uuid Agent UUID
     * @param agent_num  number of agents in the negotiation
     * @return OK if success, FAILED if failure
     */
    public String inform_message(String agent_uuid, int agent_num) throws InvalidObjectException {
        NegotiatorInfo ainfo = agents.get(agent_uuid);
        if (ainfo.id == null) {
            if (isDebug) {
                info(String.format("Agent %s does not exist", agent_uuid));
            }
            throw new InvalidObjectException(String.format("%s agent was not found. Cannot inform message ", agent_uuid));
        }
        Inform inform = new Inform(ainfo.id, "NumberOfAgents", agent_num);
        ainfo.agent.receiveMessage(ainfo.id, inform);
        return OK;
    }

    /**
     * Informs the agent about the number of negotiators in the mechanism. Uses the number sent in create_agent
     *
     * @param agent_uuid Agent UUID
     */
    public String inform_message(String agent_uuid) throws InvalidObjectException {
        NegotiatorInfo ainfo = agents.get(agent_uuid);
        if (ainfo.id == null) {
            if (isDebug) {
                info(String.format("Agent %s does not exist", agent_uuid));
            }
            throw new InvalidObjectException(String.format("%s agent was not found. Cannot inform message ", agent_uuid));
        }
        Inform inform = new Inform(ainfo.id, "NumberOfAgents", this.nAgents);
        ainfo.agent.receiveMessage(ainfo.id, inform);
        return OK;
    }

    private List<PrintStream> ignoreOutput(){
        PrintStream psSave = new PrintStream(System.out);
        PrintStream psErr = new PrintStream(System.err);
        PrintStream psNull = null;
        if (this.agentPrint) {
            return Arrays.asList(psNull, psSave, psNull);
        }
        try {
            psNull = new PrintStream(new FileOutputStream("NUL:"));
            System.setOut(psSave);
            System.setErr(psErr);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            psNull = null;
        }
        return Arrays.asList(psNull, psSave, psNull);
    }
    private void restoreOutput(List<PrintStream> lst){
        PrintStream psSave = lst.get(0);
        PrintStream psErr = lst.get(1);
        PrintStream psNull = lst.get(2);
        if (this.agentPrint) {
            return;
        }
        if (psNull == null) return;
        System.setErr(psErr);
        System.setOut(psSave);
        if (psNull != null) { psNull.close(); }
    }

    /**
     * Called by negmas when the negotiation is ended to inform the Genius agent (does not cleanup)
     *
     * @param agent_uuid Agent UUID
     * @param bid_str    the agreement if any
     * @throws ExecutionException Genius agent threw an exception while being informed about the end of the negotiation
     */
    public String on_negotiation_end(String agent_uuid, String bid_str) throws ExecutionException, InvalidObjectException {
        Bid bid = bid_str == null ? null : strToBid(agent_uuid, bid_str);
        NegotiatorInfo ainfo = agents.get(agent_uuid);
        boolean strict = ainfo.isStrict;
        boolean forcedTimeout = ainfo.isRealTimeLimit;
        NegotiationParty agent = ainfo.agent;
        List<PrintStream> lst = this.ignoreOutput();
        if (forcedTimeout && forceTimeoutInEnd) {
            ExecutorWithTimeout executor = ainfo.executor;
            try {
                executor.execute(agent.toString(), () -> agent.negotiationEnded(bid));
            } catch (TimeoutException e) {
                if (isDebug)
                    info("Negotiating party " + agent_uuid + " timed out in negotiationEnded() method.");
                return TIMEOUT;
            } catch (ExecutionException e) {
                error("Negotiating party " + agent_uuid + " threw an exception in negotiationEnded() method.");
                printException(e);
                if (strict) throw e;
                return FAILED;
            }
        } else {
            agent.negotiationEnded(bid);
            // todo: check to see if the original protocol in genius uses this return value
            // for anything
        }
        this.restoreOutput(lst);
        return OK;
    }

    /**
     * Destroys an agent (should only be called after the negotiation is ended)
     *
     * @param agent_uuid Agent UUID
     * @return OK if success, FAILED if failure, TIMEOUT if timedout
     */
    public String destroy_agent(String agent_uuid) {
        this.agents.remove(agent_uuid);
        nActiveAgents--;
        if (isDebug) {
            info(String.format("Agent %s destroyed\n", agent_uuid));
        } else {
            printStatus();
        }
        System.gc();
        return OK;
    }

    /**
     * Clears all agents and data-structures
     */
    public void clean() {
        this.agents.clear();
        nActiveAgents = this.agents.size();
        System.gc();
        if (isDebug) {
            info("All agents destroyed");
        } else {
            printStatus();
        }
    }

    public String get_name(String agent_uuid) {
        return getName(agent_uuid);
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
            info("Main thread interrupted!!");
            current.interrupt();
            return;
        }
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (current != t && t.getState() == Thread.State.RUNNABLE) {
                t.stop();
            }
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
     *
     * @param p          The protocol
     * @param domainFile Domain xml file
     * @param sProfiles  Semicolon separated profile file paths
     * @param sAgents    Semiconon separated agent class names
     * @param outputFile File used for logging
     * @return succcess/failure
     * @throws Exception Failed to run the negotiation
     */
    public boolean run_negotiation(String p, String domainFile, String sProfiles, String sAgents, String outputFile, int nSteps, int timeLimit) throws Exception {
        List<String> agents = Arrays.asList(sAgents.split(";", -1));
        List<String> profiles = Arrays.asList(sProfiles.split(";", -1));
        return this.runNegotiation(p, domainFile, profiles, agents, outputFile, nSteps, timeLimit);
    }

    /**
     * Runs a negotiation inside Genius
     *
     * @param p          The protocol
     * @param domainFile Domain xml file
     * @param profiles   list of profile files
     * @param agents     list of agent class names
     * @param outputFile File used for logging
     * @return succcess/failure
     * @throws Exception Failed to run the negotiation
     */
    private boolean runNegotiation(String p, String domainFile, List<String> profiles, List<String> agents, String outputFile, int nSteps, int timeLimit) throws Exception {
        if (!isSilent)
            print_negotiation_info(outputFile, agents, profiles, domainFile, p);
        if (p == null || domainFile == null) {
            return false;
        }
        if (profiles.size() != agents.size())
            return false;

        Global.logPreset = outputFile;
//        Protocol ns;

//        ProtocolRepItem protocol = new ProtocolRepItem(p, p, p);
        MultiPartyProtocolRepItem protocol = new MultiPartyProtocolRepItem(p, p, p, false, false);

        DomainRepItem dom = new DomainRepItem(new URL(String.format("file://%s", domainFile)));

        ProfileRepItem[] agentProfiles = new ProfileRepItem[profiles.size()];
        for (int i = 0; i < profiles.size(); i++) {
            agentProfiles[i] = new ProfileRepItem(new URL(String.format("file://%s", profiles.get(i))), dom);
            if (!isSilent)
                System.out.format("Profile: %s\n", agentProfiles[i].toString());
            if (agentProfiles[i].getDomain() != agentProfiles[0].getDomain())
                throw new IllegalArgumentException("Profiles for agent 0 and agent " + i
                        + " do not have the same domain. Please correct your profiles");
        }
        AgentID[] ids = new AgentID[agents.size()];
        for (int i = 0; i < profiles.size(); i++)
            ids[i] = new AgentID(agents.get(i));

        AgentRepItem[] agentsrep = new AgentRepItem[agents.size()];
        HashMap<AgentParameterVariable, AgentParamValue>[] agentParams = new HashMap[agentProfiles.length];
        for (int i = 0; i < agents.size(); i++) {
            agentsrep[i] = new AgentRepItem(agents.get(i), agents.get(i), agents.get(i));
            agentParams[i] = new HashMap();
            if (!isSilent)
                System.out.format("Agent Type: %s\n", agentsrep[i].toString());
        }

        PartyRepItem[] preps = new PartyRepItem[profiles.size()];
        for (int i = 0; i < profiles.size(); i++) {
            preps[i] = new PartyRepItem(agents.get(i));
        }

        Participant[] participants = new Participant[profiles.size()];
        for (int i = 0; i < profiles.size(); i++) {
            participants[i] = new Participant(ids[i],
                    preps[i],
                    agentProfiles[i],
                    null);
        }

        boolean isPrintEnabled = false;

        Deadline deadline = (timeLimit > 0) ? new Deadline(timeLimit, DeadlineType.TIME) : new Deadline(nSteps, DeadlineType.ROUND);

        SessionConfiguration config = new SessionConfiguration(protocol, null, Arrays.asList(participants), deadline, PersistentDataType.DISABLED);
        MultilateralProtocol theprotocol = TournamentManager.getProtocol(config.getProtocol());
        SessionsInfo info = new SessionsInfo(theprotocol, config.getPersistentDataType(), isPrintEnabled);
        Session session = new Session(config.getDeadline(), info);

        ExecutorWithTimeout executor = new ExecutorWithTimeout(1000L * config.getDeadline().getTimeOrDefaultTimeout());
        List<NegotiationPartyInternal> negoparties = getNegotiationParties(config, session, theprotocol);
        SessionManager sessionManager = new SessionManager(config, negoparties, session,
                executor);

        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");
        String fileName = String.format("log/Log-Session_%s", dateFormat.format(new Date()));
        FileLogger filelogger = new FileLogger(fileName);
        XmlLogger xmlLogger = new XmlLogger(new FileOutputStream(fileName + ".xml"), "Session");
        sessionManager.addListener(filelogger);
        sessionManager.addListener(xmlLogger);
        sessionManager.addListener(new ConsoleLogger());

        System.out.println("Negotiation session has started.");
        Thread t = new Thread(sessionManager);
        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * @param config The configuration being generated
     * @param session Session (running negotiation I think)
     * @return the parties for this negotiation. Converts the config into actual
     * {@link NegotiationParty}s
     * @throws RepositoryException Repository is wrong
     * @throws NegotiatorException Negotiation fails may be
     */
    private List<NegotiationPartyInternal> getNegotiationParties(MultilateralSessionConfiguration config,
                                                                 Session session, MultilateralProtocol protocol)
    // , SessionsInfo info, ExecutorWithTimeout executor
            throws RepositoryException, NegotiatorException, IOException {
        List<ParticipantRepItem> parties1 = new ArrayList<>();
        List<ProfileRepItem> profiles = new ArrayList<>();
        List<AgentID> names = new ArrayList<AgentID>();

        for (Participant participant : config.getParties()) {
            ParticipantRepItem strategy = participant.getStrategy();
            parties1.add(strategy);
            profiles.add(participant.getProfile());
            names.add(participant.getId());
            System.out.format("%s\n", participant.toString());
        }

        final List<Participant> parties = new ArrayList<Participant>(
                config.getParties());

        // add AgentIDs
        final List<Tuple<AgentID, Participant>> partiesWithId = new ArrayList<>();
        for (Participant party : parties) {
            AgentID id = AgentID.generateID(party.getStrategy().getUniqueName());
            partiesWithId.add(new Tuple<>(id, party));
        }

        List<NegotiationPartyInternal> partieslist = new ArrayList<>();

        for (Tuple<AgentID, Participant> p : partiesWithId) {
            var s = p.get2().getStrategy();
            var pro = p.get2().getProfile();
            var id = p.get1();
            //            var un = new UncertainPreferenceContainer(pro, UNCERTAINTYTYPE.PERCEIVEDUTILITY, pro);
            var sinfo = new SessionsInfo(protocol, PersistentDataType.DISABLED, false);
            var item = new NegotiationPartyInternal(s, pro, session, sinfo, id, null);
            partieslist.add(item);
        }
        return partieslist;
    }

    private boolean _run_negotiation_old(String p, String domainFile, List<String> profiles, List<String> agents, String outputFile) throws Exception {
        if (!isSilent)
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
            if (!isSilent)
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
            if (!isSilent)
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


    private TimeLineInfo getTimeline(NegotiationParty agent) {
        if (agent instanceof AbstractNegotiationParty)
            return ((AbstractNegotiationParty) agent).getTimeLine();
        return ((Agent) agent).timeline;
    }

    private Action getAction(String typeOfAction, Bid bid, AgentID agentID) {
        return typeOfAction.contains("Offer") ? new Offer(agentID, bid)
                : typeOfAction.contains("Accept") ? new Accept(agentID, bid)
                : typeOfAction.contains("EndNegotiation") ? new EndNegotiation(agentID) : null;
    }

    /// -------
    /// Helpers
    /// -------

    /**
     * Creates an executor with the remaining time of the negotiation
     *
     * @param timeline The timeline
     * @return An executor
     */
    private ExecutorWithTimeout getExecutor(TimeLineInfo timeline, long agentTimeout) {
        if (timeline instanceof DiscreteTimeline) {
            error(String.format("getExecutor() was called for a DiscreteTimeline!! Will proceed with the default for this agent %d", agentTimeout));
            return new ExecutorWithTimeout(agentTimeout);
        }
        long timeout = (long) (1000 * (timeline.getTotalTime() - timeline.getCurrentTime()));
        if (timeout >= 0) {
            if (isDebug) {
                info(String.format("Will execute with a timeout of %d ms", timeout));
            }
        } else {
            warning(String.format("Should execute with a timeout of %d ms (changed to zero)", timeout));
            timeout = 0;
        }
        return new ExecutorWithTimeout(timeout);
    }

    private HashMap<String, HashMap<String, Value>> initStrValConversion(ArrayList<Issue> issues) {
        HashMap<String, HashMap<String, Value>> string2value = new HashMap<>();
        for (Issue issue : issues) {
            String issue_name = issue.toString();
            List<ValueDiscrete> values = ((IssueDiscrete) issue).getValues();
            string2value.put(issue_name, new HashMap<>());
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
     * @return A negotiation information structure to be passed to the Genius agent.
     */
    private NegotiationInfo createNegotiationInfo(String domain_file_name, String utility_file_name, boolean real_time,
                                                  int max_time, long seed, String agent_uuid, long max_time_per_agent, boolean strict) throws IOException {
//        try {
            var domain = new DomainImpl(domain_file_name);
            var utilSpace = new AdditiveUtilitySpace(domain, utility_file_name);
            TimeLineInfo timeline;
            DeadlineType tp;
            long timeout_ms = globalTimeoutMs;
            long max_time_ms = max_time_per_agent * 1000;
            if (real_time) {
                tp = DeadlineType.TIME;
                timeline = new ContinuousTimeline(max_time);
                if (max_time < timeout_ms) {
                    timeout_ms = max_time;
                }
            } else {
                tp = DeadlineType.ROUND;
                timeline = new DiscreteTimeline(max_time);
            }
            if (max_time_ms < timeout_ms) {
                timeout_ms = max_time_ms;
            }
            var deadline = new Deadline(max_time, tp);
            var agentID = new AgentID(agent_uuid);
            var storage = new DefaultPersistentDataContainer(new Serialize(),
                    PersistentDataType.DISABLED);
            var info = new NegotiationInfo(utilSpace, deadline, timeline, seed, agentID, storage);
            var agentInfo = agents.get(agent_uuid);
            var issues = (ArrayList<Issue>) utilSpace.getDomain().getIssues();
            var striss = new HashMap<String, Issue>();
            for (Issue issue : issues) {
                striss.put(issue.toString(), issue);
            }

            agentInfo.firstAction = true;
            agentInfo.isStrict = strict;
            agentInfo.isRealTimeLimit = real_time;
            agentInfo.id = agentID;
            agentInfo.utilitySpace = utilSpace;
//            agentInfo.string2values = this.initStrValConversion(issues);
            agentInfo.string2issues = striss;
            agentInfo.timeout = timeout_ms;
            if (forceAnyTimeout)
                agentInfo.executor = new ExecutorWithTimeout(timeout_ms);
            return info;
//        } catch (Exception e) {
//            // TODO: handle exception
//            error(e.toString());
//        }
//        return null;
    }

    private String getName(String agent_uuid) {
        var agent = agents.get(agent_uuid).agent;
        if (agent instanceof AbstractNegotiationParty)
            return ((AbstractNegotiationParty) agents.get(agent_uuid).agent).getPartyId().getName();
        return ((Agent) agents.get(agent_uuid).agent).getName();
    }

    private String getDescription(String agent_uuid) {
        try {
            return agents.get(agent_uuid).agent.getDescription();
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }

    private String actionToString(Action action) {
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

    private Value get_value(Issue issue, String val) throws InvalidObjectException {
        if (issue instanceof IssueReal){
            return new ValueReal(Float.parseFloat(val));
        }
        if (issue instanceof IssueInteger){
            return new ValueInteger(Integer.parseInt(val));
        }
        if (issue instanceof IssueDiscrete){
            return new ValueDiscrete(val);
        }
        throw new InvalidObjectException(String.format("Cannot find value for %s for issue %s", val.toString(), issue.toString()));
    }

    private Bid strToBid(String agent_uuid, String bid_str) throws InvalidObjectException {
        AbstractUtilitySpace utilSpace = agents.get(agent_uuid).utilitySpace;

        if (bid_str == null) {
            warning(String.format("Received null bid ID %s", agent_uuid));
            return null;
        }
        if (bid_str.equals("")) {
            warning(String.format("Received empty bid ID %s", agent_uuid));
            return null;
        }
        String[] bid_strs = bid_str.split(ENTRY_SEP);
        HashMap<Integer, Value> vals = new HashMap<>();
        NegotiatorInfo ag = agents.get(agent_uuid);
        for (String str : bid_strs) {
            String[] vs = str.split(INTERNAL_SEP);
            String issue_name = vs[0];
            String val = vs.length > 1 ? vs[1] : "";
            Issue issue = ag.string2issues.get(issue_name);
            vals.put(issue.getNumber(), get_value(issue, val));
                    //ag.string2values.get(issue_name).get(val));
        }
        Bid bid;
        try {
            bid = new Bid(utilSpace.getDomain(), vals);
        } catch (Exception e) {
            bid = null;
            warning(e.toString());
            printException(e);
        }
        return bid;
    }

    private void printStatus() {
        if (isSilent)
            return;
        System.out.format("\r%06d agents (%06d active) [%06d ids]: %09d received, %09d sent", nTotalAgents,
                nActiveAgents, this.agents.size(), nTotalOffers, nTotalResponses);
        System.out.flush();
    }

    private void info(String s) {
        if (logging)
            logger.info(s);
        if (isDebug && !isSilent) {
            System.out.println(s);
            System.out.flush();
        }
    }

    private void warning(String s) {
        if (logging)
            logger.warning(s);
        if (isDebug && !isSilent) {
            System.out.println(s);
            System.out.flush();
        }
    }

    private void error(String s) {
        if (logging)
            logger.log(Level.SEVERE, s);
        if (isDebug && !isSilent) {
            System.out.println(s);
            System.out.flush();
        }
    }

    private void printException(Exception e) {
        if (!isSilent)
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
        String msg = String.format("Gateway %s to python started at port %d listening to port %d [%s: %dms]\n", version(), port,
                listening_port, forceTimeout ? "forcing timeout" : "no timeout", this.globalTimeoutMs);
        if (!isSilent)
            System.out.print(msg);
        info(msg);
    }

    public void shutdownPy4jServer() {
        server.shutdown();
    }

    public static class Serialize implements Serializable {
    }
}
