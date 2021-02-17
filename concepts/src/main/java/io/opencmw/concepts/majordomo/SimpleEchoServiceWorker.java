package io.opencmw.concepts.majordomo;

import io.opencmw.rbac.BasicRbacRole;

/**
* Majordomo Protocol worker example. Uses the mdwrk API to hide all OpenCmwProtocol aspects
*
*/
public class SimpleEchoServiceWorker { // NOPMD - nomen est omen

    private SimpleEchoServiceWorker() {
        // private helper/test class
    }

    public static void main(String[] args) {
        MajordomoWorker workerSession = new MajordomoWorker("tcp://localhost:5556", "echo", BasicRbacRole.ADMIN);
        // workerSession.setDaemon(true); // use this if running in another app that controls threads
        workerSession.registerHandler(input -> input); //  output = input : echo service is complex :-)
        workerSession.start();
    }
}
