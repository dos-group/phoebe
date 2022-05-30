package de.tu_berlin.dos.phoebe.execution.twres;

import de.tu_berlin.dos.phoebe.execution.Job;
import de.tu_berlin.dos.phoebe.managers.ClientsManager;
import de.tu_berlin.dos.phoebe.managers.DataManager;
import de.tu_berlin.dos.phoebe.structures.OrderedProperties;
import de.tu_berlin.dos.phoebe.structures.PropertyTree;
import de.tu_berlin.dos.phoebe.utils.EventTimer;
import de.tu_berlin.dos.phoebe.utils.FileManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
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
    public final int evalInt;
    public final int avgWindow;
    public final double avgLatConst;
    public final String dataPath;
    public final PropertyTree k8s;
    public final String brokerList;
    public final String tarConsTopic;
    public final String tarProdTopic;

    public final String genType;
    public final String limiterType;
    public final float limiterMaxNoise;
    public final int amplitude;
    public final int verticalPhase;
    public final int period;
    public final String fileName;

    public final int maxScaleOut;
    public final int minScaleOut;

    public final Job job;
    public final List<Integer> failures = new ArrayList<>();
    public final AtomicInteger reconCount = new AtomicInteger(0);

    public final DataManager dm;
    public final ClientsManager cm;
    public final EventTimer et;

    public final ExecutorService executor = Executors.newCachedThreadPool();

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    private Context(String propertiesFile) throws Exception {

        OrderedProperties props = FileManager.GET.resource(propertiesFile, OrderedProperties.class);

        // get general properties
        this.expId = Integer.parseInt(props.getProperty("general.expId"));
        this.expLen = Integer.parseInt(props.getProperty("general.expLen"));
        this.evalInt = Integer.parseInt(props.getProperty("general.evalInt"));
        this.avgWindow = Integer.parseInt(props.getProperty("general.avgWindow"));
        this.avgLatConst = Double.parseDouble(props.getProperty("general.avgLatConst"));
        this.dataPath = props.getProperty("general.dataPath");
        this.brokerList = props.getProperty("general.brokerList");
        this.tarConsTopic = props.getProperty("general.consumerTopic");
        this.tarProdTopic = props.getProperty("general.producerTopic");
        String entryClass = props.getProperty("general.entryClass");
        int chkInterval = Integer.parseInt(props.getProperty("general.chkInterval"));
        this.job = new Job("twres", this.brokerList, this.tarConsTopic, this.tarProdTopic, entryClass, chkInterval);
        PropertyTree misc = props.getPropertyList("misc").find("args");
        if (misc != null) misc.forEach(e -> job.extraArgs.put(e.key, e.value));
        job.setScaleOut(6);

        this.genType = props.getProperty("general.generatorType");
        this.limiterType = props.getProperty("general.limiterType");
        this.limiterMaxNoise = Float.parseFloat(props.getProperty("general.limiterMaxNoise"));
        this.amplitude = Integer.parseInt(props.getProperty("general.amplitude"));
        this.verticalPhase = Integer.parseInt(props.getProperty("general.verticalPhase"));
        this.period = Integer.parseInt(props.getProperty("general.period"));
        this.fileName = props.getProperty("general.fileName");
        this.maxScaleOut = Integer.parseInt(props.getProperty("general.maxScaleOut"));
        this.minScaleOut = Integer.parseInt(props.getProperty("general.minScaleOut"));
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
        this.cm = ClientsManager.create(
            namespace, flinkBaseUrl, jarPath, sourceRegex, targetDirectory, promBaseUrl, analyticsPyBaseUrl, generatorList);
        // create data manager
        this.dm = DataManager.create();
        // create timer manager used in profiling
        this.et = new EventTimer();
    }

    /******************************************************************************
     * INSTANCE BEHAVIOURS
     ******************************************************************************/

    @Override
    public void close() throws Exception {

        this.cm.close();
        this.executor.shutdownNow();
    }
}
