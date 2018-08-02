package exactlyonce;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class Test {
    public void test() throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {

        KeyStore ks = KeyStore.getInstance("JKS");
        // make sure you close this stream properly (not shown here for brevity)
        InputStream trustStore = new FileInputStream("client.truststore");
        ks.load(trustStore,"password123".toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);

        SslContextBuilder builder = SslContextBuilder
                .forClient()
                .sslProvider(SslProvider.OPENSSL)
                .trustManager(tmf)
                // only if you use client authentication
                .keyManager(new File("client.crt"), new File("client.key"));


    }

}
