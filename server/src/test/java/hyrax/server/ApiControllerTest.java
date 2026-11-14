package hyrax.server;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = "hyrax.agent-token=secret")
@AutoConfigureMockMvc
@Import(TestDatabase.class)
class ApiControllerTest {

    private static final String BODY = """
            {"node": "node-9", "agentVersion": "0.1.0", "attempts": [],
             "gpus": [{"index": 0, "uuid": "GPU-x", "name": "RTX 4090", "utilization": 97,
                       "memoryUsedMb": 20000, "memoryTotalMb": 24564, "temperatureC": 71, "powerW": 400.5,
                       "processes": [{"pid": 42, "type": "compute", "user": "alice",
                                      "command": "python train.py", "memoryMb": 19900}]}]}
            """;

    @Autowired
    MockMvc mvc;

    @Test
    void heartbeatShowsUpInNodes() throws Exception {
        mvc.perform(post("/api/agent/heartbeat")
                        .header("Authorization", "Bearer secret")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(BODY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.run").isEmpty())
                .andExpect(jsonPath("$.cancel").isEmpty());

        mvc.perform(get("/api/nodes"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].node").value("node-9"))
                .andExpect(jsonPath("$[0].online").value(true))
                .andExpect(jsonPath("$[0].gpus[0].memoryUsedMb").value(20000))
                .andExpect(jsonPath("$[0].gpus[0].processes[0].command").value("python train.py"));
    }

    @Test
    void heartbeatWithoutTokenIsRejected() throws Exception {
        mvc.perform(post("/api/agent/heartbeat").contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isUnauthorized());
    }
}
