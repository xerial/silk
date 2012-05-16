---
layout: post
title: "Sign project with GPG"
description: ""
category: 
tags: []
---
{% include JB/setup %}

Create a GPG key

    $ gpg --gen-key
    gpg (GnuPG) 2.0.4-svn0; Copyright (C) 2007 Free Software Foundation, Inc.
    This program comes with ABSOLUTELY NO WARRANTY.
    This is free software, and you are welcome to redistribute it
    under certain conditions. See the file COPYING for details.
    
    Please select what kind of key you want:
    (1) DSA and Elgamal (default)
    (2) DSA (sign only)
    (5) RSA (sign only)
    Your selection? 
    DSA keypair will have 1024 bits.
    ELG keys may be between 1024 and 4096 bits long.
    What keysize do you want? (2048) 
    Requested keysize is 2048 bits
    Please specify how long the key should be valid.
    0 = key does not expire
    <n>  = key expires in n days
    <n>w = key expires in n weeks
    <n>m = key expires in n months
    <n>y = key expires in n years
    Key is valid for? (0) 
    Key does not expire at all
    Is this correct? (y/N) 
    Key is valid for? (0) 
    Key does not expire at all
    Is this correct? (y/N) y
    
    You need a user ID to identify your key; the software constructs the user ID
    from the Real Name, Comment and Email Address in this form:
    "Heinrich Heine (Der Dichter) <heinrichh@duesseldorf.de>"
    
    Real name: Taro L. Saito
    Email address: leo@xerial.org
    Comment: 
    You selected this USER-ID:
    "Taro L. Saito <leo@xerial.org>"
    
    Change (N)ame, (C)omment, (E)mail or (O)kay/(Q)uit? O
    You need a Passphrase to protect your secret key.
    
    We need to generate a lot of random bytes. It is a good idea to perform
    some other action (type on the keyboard, move the mouse, utilize the
    disks) during the prime generation; this gives the random number
    generator a better chance to gain enough entropy.
    
    gpg: key 12FA6E33 marked as ultimately trusted
    public and secret key created and signed.
    
    gpg: checking the trustdb
    gpg: 3 marginal(s) needed, 1 complete(s) needed, PGP trust model
    gpg: depth: 0  valid:   1  signed:   0  trust: 0-, 0q, 0n, 0m, 0f, 1u
    pub   1024D/12FA6E33 2012-05-16
    Key fingerprint = 4122 8709 5ABC 210B 25BD  0399 1645 903F 12FA 6E33
    uid                  Taro L. Saito <leo@xerial.org>
    sub   2048g/A5045838 2012-05-16

Add xsbt-gpg-plugin settings to your sbt build file:

    resolvers += Resolver.url("sbt-plugin-releases", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns)
    
    addSbtPlugin("com.jsuereth" % "xsbt-gpg-plugin" % "0.6")
