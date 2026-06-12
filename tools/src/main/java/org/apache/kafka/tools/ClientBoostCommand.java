/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.RegisterClientBoostResult;
import org.apache.kafka.clients.admin.UnregisterClientBoostResult;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public abstract class ClientBoostCommand {

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    private static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        ClientBoostCommandOptions opts = new ClientBoostCommandOptions(args);
        
        Properties config = opts.commandConfig();
        opts.bootstrapServer().ifPresent(server -> config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server));

        try (Admin adminClient = Admin.create(config)) {
            List<String> clientIds = opts.clientIds();
            for (String clientId : clientIds) {
                if (opts.hasRegisterOption()) {
                    registerClientBoost(adminClient, clientId);
                } else if (opts.hasUnregisterOption()) {
                    unregisterClientBoost(adminClient, clientId);
                }
            }
        }
    }

    private static void registerClientBoost(Admin admin, String clientId) {
        try {
            System.out.print("Processing [" + clientId + "] ... ");
            RegisterClientBoostResult result = admin.registerClientBoost(clientId);
            result.all().get();
            System.out.println("Success");
        } catch (ExecutionException e) {
            System.out.println("Failure");
            System.err.println("   Cause: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
        } catch (InterruptedException e) {
            System.out.println("Stopped");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.out.println("Unknown Failure");
            e.printStackTrace();
        }
    }

    private static void unregisterClientBoost(Admin admin, String clientId) {
        try {
            System.out.print("Processing [" + clientId + "] ... ");
            UnregisterClientBoostResult result = admin.unregisterClientBoost(clientId);
            result.all().get();
            System.out.println("Success");
        } catch (ExecutionException e) {
            System.out.println("Failure");
            System.err.println("   Cause: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
        } catch (InterruptedException e) {
            System.out.println("Stopped");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.out.println("Unknown Failure");
            e.printStackTrace();
        }
    }

    public static final class ClientBoostCommandOptions extends CommandDefaultOptions {
        private final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;
        private final ArgumentAcceptingOptionSpec<String> commandConfigOpt;
        private final OptionSpecBuilder registerOpt;
        private final OptionSpecBuilder unregisterOpt;
        private final ArgumentAcceptingOptionSpec<String> clientIdOpt;

        public ClientBoostCommandOptions(String[] args) {
            super(args);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The Kafka server to connect to.")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);
            commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client.")
                .withRequiredArg()
                .describedAs("command config property file")
                .ofType(String.class);

            registerOpt = parser.accepts("register", "Register client boost.");
            unregisterOpt = parser.accepts("unregister", "Unregister client boost.");
            clientIdOpt = parser.accepts("client-id", "The client ID to register or unregister. This option can be used multiple times.")
                .withRequiredArg()
                .describedAs("client-id")
                .ofType(String.class);

            options = parser.parse(args);
            checkArgs();
        }

        public Boolean has(OptionSpec<?> builder) {
            return options.has(builder);
        }

        public <A> Optional<A> valueAsOption(OptionSpec<A> option) {
            if (has(option)) {
                return Optional.of(options.valueOf(option));
            } else {
                return Optional.empty();
            }
        }

        public Boolean hasRegisterOption() {
            return has(registerOpt);
        }

        public Boolean hasUnregisterOption() {
            return has(unregisterOpt);
        }

        public Optional<String> bootstrapServer() {
            return valueAsOption(bootstrapServerOpt);
        }

        public Properties commandConfig() throws IOException {
            if (has(commandConfigOpt)) {
                return Utils.loadProps(options.valueOf(commandConfigOpt));
            } else {
                return new Properties();
            }
        }

        public List<String> clientIds() {
            return options.valuesOf(clientIdOpt);
        }

        public void checkArgs() {
            if (args.length == 0)
                CommandLineUtils.printUsageAndExit(parser, "Register or unregister client boost.");

            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to register or unregister client boost.");

            long actions = Arrays.asList(registerOpt, unregisterOpt).stream().filter(options::has).count();
            if (actions != 1)
                CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --register or --unregister");

            CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt, clientIdOpt);
        }
    }
}