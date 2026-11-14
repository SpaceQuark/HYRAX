package hyrax.server;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/** Pushes the full cluster snapshot to every open dashboard connection. */
@Component
public class LiveStream {

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final ClusterState cluster;

    public LiveStream(ClusterState cluster) {
        this.cluster = cluster;
    }

    /** A node that stops heartbeating triggers no publish of its own, so republish on a timer. */
    @Scheduled(fixedRate = 3000)
    void tick() {
        if (!emitters.isEmpty()) {
            publish(cluster.snapshot());
        }
    }

    public SseEmitter subscribe(List<ClusterState.Node> current) {
        SseEmitter emitter = new SseEmitter(0L);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(error -> emitters.remove(emitter));
        emitters.add(emitter);
        send(emitter, current);
        return emitter;
    }

    public void publish(List<ClusterState.Node> snapshot) {
        emitters.forEach(emitter -> send(emitter, snapshot));
    }

    private void send(SseEmitter emitter, List<ClusterState.Node> snapshot) {
        try {
            emitter.send(SseEmitter.event().name("nodes").data(snapshot));
        } catch (IOException | IllegalStateException gone) {
            emitters.remove(emitter);
        }
    }
}
