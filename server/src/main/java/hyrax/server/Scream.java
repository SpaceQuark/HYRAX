package hyrax.server;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.api.bolt.App;
import com.slack.api.bolt.AppConfig;
import com.slack.api.bolt.context.builtin.ActionContext;
import com.slack.api.bolt.request.builtin.BlockActionRequest;
import com.slack.api.bolt.response.Response;
import com.slack.api.bolt.socket_mode.SocketModeApp;
import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.chat.ChatUpdateResponse;
import com.slack.api.socket_mode.SocketModeClient;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

/**
 * The Slack side of alerting: posts screams and handles their buttons over Socket Mode.
 * Only exists when a bot token is configured; without one, alerts are recorded but nobody is pinged.
 */
@Component
@ConditionalOnExpression("!'${hyrax.slack.bot-token:}'.isEmpty()")
public class Scream {

    private static final Logger log = LoggerFactory.getLogger(Scream.class);

    private final Alerts alerts;
    private final ObjectMapper json;
    private final String appToken;
    private final String labChannel;
    private final Duration snooze;
    private final App app;
    private SocketModeApp socket;

    public Scream(
            Alerts alerts,
            ObjectMapper json,
            @Value("${hyrax.slack.bot-token}") String botToken,
            @Value("${hyrax.slack.app-token}") String appToken,
            @Value("${hyrax.slack.lab-channel}") String labChannel,
            @Value("${hyrax.slack.snooze}") Duration snooze) {
        this.alerts = alerts;
        this.json = json;
        this.appToken = appToken;
        this.labChannel = labChannel;
        this.snooze = snooze;
        this.app = new App(AppConfig.builder().singleTeamBotToken(botToken).build());
        app.blockAction(ScreamMessage.CLAIM_ACTION, this::claim);
        app.blockAction(ScreamMessage.SNOOZE_ACTION, this::snooze);
    }

    @PostConstruct
    void connect() throws Exception {
        socket = new SocketModeApp(appToken, SocketModeClient.Backend.JavaWebSocket, app);
        socket.startAsync();
        log.info("Slack connected; unowned alerts go to channel {}", labChannel);
    }

    @PreDestroy
    void disconnect() throws Exception {
        if (socket != null) {
            socket.close();
        }
    }

    /** Pings the owner by DM if there is one, otherwise the lab channel. */
    public void send(Alerts.Alert alert) {
        Instant now = Instant.now();
        String target = alert.ownerSlackId() != null ? alert.ownerSlackId() : labChannel;
        try {
            String blocks = json.writeValueAsString(ScreamMessage.withButtons(alert, now, snooze));
            ChatPostMessageResponse response = app.client().chatPostMessage(request -> request
                    .channel(target)
                    .text(ScreamMessage.fallbackText(alert, now))
                    .blocksAsString(blocks));
            if (response.isOk()) {
                alerts.recordMessage(alert.id(), response.getChannel(), response.getTs());
            } else {
                log.warn("Slack refused alert {} for {}: {}", alert.id(), target, response.getError());
            }
        } catch (IOException | SlackApiException error) {
            log.warn("Could not send alert {} to Slack: {}", alert.id(), error.getMessage());
        }
    }

    private Response claim(BlockActionRequest request, ActionContext context) {
        String user = request.getPayload().getUser().getId();
        clicked(request).ifPresent(alert -> {
            alerts.claim(alert, user);
            log.info("Alert {} ({} pid {}) claimed by {}", alert.id(), alert.node(), alert.pid(), user);
            replace(request, alert,
                    "Claimed by <@%s>. Future alerts for this process go to their DMs.".formatted(user));
        });
        return context.ack();
    }

    private Response snooze(BlockActionRequest request, ActionContext context) {
        String user = request.getPayload().getUser().getId();
        clicked(request).ifPresent(alert -> {
            Instant until = Instant.now().plus(snooze);
            alerts.remindAt(alert.id(), until);
            log.info("Alert {} snoozed by {} until {}", alert.id(), user, until);
            replace(request, alert,
                    "Snoozed by <@%s>. HYRAX will ask again <!date^%d^{date_short_pretty} at {time}|in %s> if it is still idle."
                            .formatted(user, until.getEpochSecond(), ScreamMessage.words(snooze)));
        });
        return context.ack();
    }

    private Optional<Alerts.Alert> clicked(BlockActionRequest request) {
        String value = request.getPayload().getActions().get(0).getValue();
        try {
            return alerts.find(Long.parseLong(value));
        } catch (NumberFormatException notAnAlertId) {
            return Optional.empty();
        }
    }

    private void replace(BlockActionRequest request, Alerts.Alert alert, String outcome) {
        Instant now = Instant.now();
        List<Map<String, Object>> blocks = ScreamMessage.resolved(alert, now, outcome);
        try {
            String blocksJson = json.writeValueAsString(blocks);
            ChatUpdateResponse response = app.client().chatUpdate(update -> update
                    .channel(request.getPayload().getChannel().getId())
                    .ts(request.getPayload().getMessage().getTs())
                    .text(ScreamMessage.fallbackText(alert, now) + " " + outcome)
                    .blocksAsString(blocksJson));
            if (!response.isOk()) {
                log.warn("Slack refused the update for alert {}: {}", alert.id(), response.getError());
            }
        } catch (IOException | SlackApiException error) {
            log.warn("Could not update the Slack message for alert {}: {}", alert.id(), error.getMessage());
        }
    }
}
