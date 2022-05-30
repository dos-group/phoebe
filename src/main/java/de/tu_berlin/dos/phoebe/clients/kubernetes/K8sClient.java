package de.tu_berlin.dos.phoebe.clients.kubernetes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.fabric8.chaosmesh.client.ChaosMeshClient;
import io.fabric8.chaosmesh.client.DefaultChaosMeshClient;
import io.fabric8.chaosmesh.v1alpha1.NetworkChaos;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.*;
import io.fabric8.kubernetes.client.Watcher.Action;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.utils.HttpClientUtils;
import io.fabric8.kubernetes.client.utils.Utils;
import okhttp3.OkHttpClient;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class K8sClient implements AutoCloseable {

    /******************************************************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************************************************/

    static class Listener implements ExecListener {

        private final CompletableFuture<String> data;
        private final ByteArrayOutputStream out;

        public Listener(CompletableFuture<String> data, ByteArrayOutputStream out) {

            this.data = data;
            this.out = out;
        }

        @Override
        public void onOpen(okhttp3.Response response) {

            LOG.info("Reading data... " + response.message());
        }

        @Override
        public void onFailure(Throwable t, okhttp3.Response response) {

            LOG.error(t.getMessage() + " " + response.message());
            data.completeExceptionally(t);
        }

        @Override
        public void onClose(int code, String reason) {

            LOG.info("Exit with: " + code + " and with reason: " + reason);
            data.complete(out.toString());
        }
    }

    /******************************************************************************************************************
     * CLASS STATE
     ******************************************************************************************************************/

    private static final Logger LOG = LogManager.getLogger(K8sClient.class);

    /******************************************************************************************************************
     * CLASS BEHAVIOUR(S)
     ******************************************************************************************************************/

    public static String display(HasMetadata item) {

        StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        if (Utils.isNotNullOrEmpty(item.getKind())) {

            sb.append("Kind: ").append(item.getKind());
        }
        if (Utils.isNotNullOrEmpty(item.getMetadata().getName())) {

            sb.append(", Name: ").append(item.getMetadata().getName());
        }
        if (item.getMetadata().getLabels() != null && !item.getMetadata().getLabels().isEmpty()) {

            sb.append(", Labels: [ ");
            for (Map.Entry<String,String> entry : item.getMetadata().getLabels().entrySet()) {

                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append(" ");
            }
            sb.append("]");
        }
        sb.append(" ]");
        return sb.toString();
    }

    public static <T extends HasMetadata> void watchFor(Resource<T> resource, Action event) throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        try (Watch ignored = resource.watch(new Watcher<>() {

            @Override
            public void eventReceived(Action action, T hasMetadata) {

                if (action == event) {

                    LOG.info(hasMetadata.getKind() + " (" + hasMetadata.getMetadata().getName() + ") " + action);
                    latch.countDown();
                }
            }

            @Override
            public void onClose(WatcherException e) {

                if (e != null) {

                    e.printStackTrace();
                    LOG.error(e.getMessage(), e);
                }
            }
        })) {

            latch.await();
        }
    }

    /******************************************************************************************************************
     * OBJECT STATE
     ******************************************************************************************************************/

    private final ChaosMeshClient chaos;
    private final KubernetesClient k8s;
    public final Gson gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();

    /******************************************************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************************************************/

    public K8sClient() {

        Config config = new ConfigBuilder().build();
        OkHttpClient okHttpClient = HttpClientUtils.createHttpClient(config);
        this.chaos = new DefaultChaosMeshClient(okHttpClient, config);
        this.k8s = new DefaultKubernetesClient(okHttpClient, config);
    }

    public K8sClient(String masterUrl, String tokenBearer) {

        Config config =
            new ConfigBuilder()
                .withTrustCerts(true)
                .withMasterUrl(masterUrl)
                .withOauthToken(tokenBearer)
                .build();
        OkHttpClient okHttpClient = HttpClientUtils.createHttpClient(config);
        this.chaos = new DefaultChaosMeshClient(okHttpClient, config);
        this.k8s = new DefaultKubernetesClient(okHttpClient, config);
    }

    /******************************************************************************************************************
     * OBJECT BEHAVIOUR(S)
     ******************************************************************************************************************/

    public Namespace createOrReplaceNamespace(String namespaceName) throws Exception {

        Namespace namespace = k8s.namespaces().createOrReplace(
            new NamespaceBuilder()
                .withNewMetadata()
                .withName(namespaceName)
                .endMetadata()
                .build());
        K8sClient.watchFor(k8s.namespaces().withName(namespaceName), Action.ADDED);
        return namespace;
    }

    public void deleteNamespace(String namespaceName) throws Exception {

        if (k8s.namespaces().withName(namespaceName).get() != null) {

            k8s.pods().inNamespace(namespaceName).withGracePeriod(0).delete();
            k8s.namespaces().withName(namespaceName).withGracePeriod(0).delete();
            K8sClient.watchFor(k8s.namespaces().withName(namespaceName), Action.DELETED);
        }
    }

    public NetworkChaos createOrReplaceNetworkChaos(String namespace, NetworkChaos request) {

        return this.chaos.networkChaos().inNamespace(namespace).createOrReplace(request);
    }

    public List<HasMetadata> createOrReplaceResources(String namespaceName, String fileName) throws Exception {

        File file = new File(fileName);
        String resourceStr = FileUtils.readFileToString(file, StandardCharsets.UTF_8);

        List<HasMetadata> resources = k8s.resourceList(resourceStr).inNamespace(namespaceName).createOrReplace();
        for (HasMetadata resource : resources) {

            String resourceName = resource.getMetadata().getName();
            switch (resource.getKind()) {

                case "StatefulSet": {

                    k8s.apps().statefulSets().inNamespace(namespaceName).withName(resourceName).waitUntilReady(5, TimeUnit.MINUTES);
                    break;
                }
                case "Deployment": {

                    k8s.apps().deployments().inNamespace(namespaceName).withName(resourceName).waitUntilReady(5, TimeUnit.MINUTES);
                    break;
                }
            }
        }
        return resources;
    }

    public List<HasMetadata> createOrReplaceResources(String namespaceName, List<String> fileNames) throws Exception {

        List<HasMetadata> resources = new ArrayList<>();
        for (String fileName : fileNames) resources.addAll(this.createOrReplaceResources(namespaceName, fileName));
        return resources;
    }

    private ExecWatch execCmd(Pod pod, CompletableFuture<String> data, String... command) {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        return k8s.pods()
            .inNamespace(pod.getMetadata().getNamespace())
            .withName(pod.getMetadata().getName())
            .writingOutput(out)
            .writingError(out)
            .usingListener(new Listener(data, out))
            .exec(command);
    }

    public String execCommandOnPod(String podName, String namespace, String... cmd) throws Exception {

        Pod pod = k8s.pods().inNamespace(namespace).withName(podName).get();
        LOG.info(String.format("Running command: [%s] on pod [%s] in namespace [%s]%n",
                 Arrays.toString(cmd), pod.getMetadata().getName(), namespace));

        CompletableFuture<String> data = new CompletableFuture<>();
        try (ExecWatch execWatch = execCmd(pod, data, cmd)) {

            return data.get(10, TimeUnit.SECONDS);
        }
    }

    public List<String> getPods(String namespace, String labelKey, String labelVal) {

        List<String> podNames = new ArrayList<>();
        for (Pod pod : k8s.pods().inNamespace(namespace).withLabel(labelKey, labelVal).list().getItems()) {

            podNames.add(pod.getMetadata().getName());
        }
        return podNames;
    }

    @Override
    public void close() {

        this.k8s.close();
    }
}
