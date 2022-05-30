package de.tu_berlin.dos.phoebe.execution.baseline;

import de.tu_berlin.dos.phoebe.execution.Job;
import de.tu_berlin.dos.phoebe.managers.ClientsManager;
import de.tu_berlin.dos.phoebe.managers.DataManager;
import de.tu_berlin.dos.phoebe.structures.OrderedProperties;
import de.tu_berlin.dos.phoebe.structures.PropertyTree;
import de.tu_berlin.dos.phoebe.utils.EventTimer;
import de.tu_berlin.dos.phoebe.utils.FileManager;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Context implements AutoCloseable {

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = LogManager.getLogger(Context.class);

    /******************************************************************************
     * CLASS BEHAVIOURS
     ******************************************************************************/

    public static Context create(String propertiesFile) throws Exception {

        return new Context(propertiesFile);
    }

    public static List<Integer> calcRange(int max, int min, int count) {

        List<Integer> range = new ArrayList<>();
        int step = (int) (((max - min) * 1.0 / (count - 1)) + 0.5);
        Stream.iterate(min, i -> i + step).limit(count).forEach(range::add);
        return range;
    }

    /******************************************************************************
     * INSTANCE VARIABLES
     ******************************************************************************/

    public final int expId;
    public final int expLen;
    public final int avgWindow;
    public final String dataPath;
    public final PropertyTree k8s;
    public final String brokerList;
    public final String tarConsTopic;
    public final String tarProdTopic;
    public final String generatorType;
    public final String limiterType;
    public final float limiterMaxNoise;
    public final int amplitude;
    public final int verticalPhase;
    public final int period;
    public final String fileName;
    public final List<Job> jobs = new ArrayList<>();
    public final List<Integer> failures = new ArrayList<>();

    public final DataManager dataManager;
    public final ClientsManager clientsManager;
    public final EventTimer eventTimer;

    public final ExecutorService executor = Executors.newCachedThreadPool();

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    private Context(String propertiesFile) throws Exception {

        OrderedProperties props = FileManager.GET.resource(propertiesFile, OrderedProperties.class);

        // get general properties
        this.expId = Integer.parseInt(props.getProperty("general.expId"));
        this.expLen = Integer.parseInt(props.getProperty("general.expLen"));
        this.avgWindow = Integer.parseInt(props.getProperty("general.avgWindow"));
        this.dataPath = props.getProperty("general.dataPath");
        this.brokerList = props.getProperty("general.brokerList");
        this.tarConsTopic = props.getProperty("general.consumerTopic");
        this.tarProdTopic = props.getProperty("general.producerTopic");
        String entryClass = props.getProperty("general.entryClass");
        int chkInterval = Integer.parseInt(props.getProperty("general.chkInterval"));
        List<Integer> scaleOuts = Arrays.stream(props.getProperty("general.scaleOuts").split(",")).map(Integer::parseInt).collect(Collectors.toList());
        for (int scaleOut : scaleOuts) {

            String jobName = String.format("baseline_%d", scaleOut);
            Job job = new Job(jobName, this.brokerList, this.tarConsTopic, this.tarProdTopic, entryClass, chkInterval);
            PropertyTree misc = props.getPropertyList("misc").find("args");
            if (misc != null) misc.forEach(e -> job.extraArgs.put(e.key, e.value));
            job.setScaleOut(scaleOut);
            this.jobs.add(job);
        }

        this.generatorType = props.getProperty("general.generatorType");
        this.limiterType = props.getProperty("general.limiterType");
        this.limiterMaxNoise = Float.parseFloat(props.getProperty("general.limiterMaxNoise"));
        this.amplitude = Integer.parseInt(props.getProperty("general.amplitude"));
        this.verticalPhase = Integer.parseInt(props.getProperty("general.verticalPhase"));
        this.period = Integer.parseInt(props.getProperty("general.period"));
        this.fileName = props.getProperty("general.fileName");
        int numFailures = Integer.parseInt(props.getProperty("general.numFailures"));
        failures.addAll(calcRange(this.expLen - 1200, 1200, numFailures));
        // get kubernetes properties
        this.k8s = props.getPropertyList("k8s");
        // create clients manager
        String namespace = this.k8s.find("namespace").value;
        String flinkBaseUrl = props.getProperty("clients.flink");
        String jarPath = props.getProperty("general.jarPath");
        String sourceRegex = props.getProperty("general.sourceRegex");
        String targetDirectory = props.getProperty("general.targetDirectory");
        String promBaseUrl = props.getProperty("clients.prometheus");
        String analyticsPyBaseUrl = props.getProperty("clients.analyticsPy");
        String generatorList = props.getProperty("clients.generatorList");
        this.clientsManager = ClientsManager.create(
            namespace, flinkBaseUrl, jarPath, sourceRegex, targetDirectory, promBaseUrl, analyticsPyBaseUrl, generatorList);
        // create data manager
        this.dataManager = DataManager.create();
        // create timer manager used in profiling
        this.eventTimer = new EventTimer();
    }

    /******************************************************************************
     * INSTANCE BEHAVIOURS
     ******************************************************************************/

    @Override
    public void close() throws Exception {

        this.clientsManager.close();
        this.executor.shutdownNow();
    }
}
