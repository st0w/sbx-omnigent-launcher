/*
 * keychain-token — print one generic password from the login keychain.
 *
 * Build + install:  sh tools/build-keychain-token.sh
 *
 * WHY THIS IS A COMPILED BINARY AND NOT A SHELL SCRIPT
 * ----------------------------------------------------
 * A keychain item's ACL authorizes the RUNNING PROCESS's code identity.
 * Run a shell script and that identity is the interpreter (/bin/sh) —
 * the script is just an argument — so "trust my script" would trust
 * every script on the machine. Only a signed binary has an identity of
 * its own, so only a binary can be trusted individually.
 *
 * WHY IT EXISTS AT ALL
 * --------------------
 * The runner re-reads the publish token AT PUSH TIME (see TASKS.md #43)
 * so a secret rotated mid-run is picked up. That read must therefore
 * hit the live store, and must not raise a GUI prompt — a dialog hours
 * into an unattended run blocks the publish with nobody there to click
 * it. Granting `/usr/bin/security` standing access to the item would
 * work, but then anything on the host that shells out to `security`
 * reads it silently too. Trusting THIS binary instead keeps the item
 * unreadable to `security` and to every other program.
 *
 * Despite the name it is generic: -s/-a select any generic-password
 * item, so a second pipeline can use it with its own item and its own
 * ACL entry.
 *
 * Usage: keychain-token -s <service> [-a <account>]
 */
#include <CoreFoundation/CoreFoundation.h>
#include <Security/Security.h>
#include <stdio.h>
#include <unistd.h>

static void usage(const char *argv0) {
    fprintf(stderr, "usage: %s -s <service> [-a <account>]\n", argv0);
}

int main(int argc, char **argv) {
    const char *service = NULL, *account = NULL;
    int c;

    while ((c = getopt(argc, argv, "s:a:")) != -1) {
        if (c == 's') service = optarg;
        else if (c == 'a') account = optarg;
        else { usage(argv[0]); return 2; }
    }
    if (!service) { usage(argv[0]); return 2; }

    CFStringRef svc = CFStringCreateWithCString(
        NULL, service, kCFStringEncodingUTF8);
    CFMutableDictionaryRef q = CFDictionaryCreateMutable(
        NULL, 0, &kCFTypeDictionaryKeyCallBacks,
        &kCFTypeDictionaryValueCallBacks);
    CFDictionarySetValue(q, kSecClass, kSecClassGenericPassword);
    CFDictionarySetValue(q, kSecAttrService, svc);
    if (account) {
        CFStringRef acct = CFStringCreateWithCString(
            NULL, account, kCFStringEncodingUTF8);
        CFDictionarySetValue(q, kSecAttrAccount, acct);
        CFRelease(acct);
    }
    CFDictionarySetValue(q, kSecReturnData, kCFBooleanTrue);
    CFDictionarySetValue(q, kSecMatchLimit, kSecMatchLimitOne);

    CFTypeRef out = NULL;
    OSStatus st = SecItemCopyMatching((CFDictionaryRef)q, &out);
    CFRelease(q);
    CFRelease(svc);

    if (st != errSecSuccess) {
        /* errSecItemNotFound = wrong -s/-a. Anything auth-shaped means
         * this binary is not in that item's trusted-application list —
         * re-run the one-time grant in the README. */
        CFStringRef m = SecCopyErrorMessageString(st, NULL);
        char buf[256] = {0};
        if (m) {
            CFStringGetCString(m, buf, sizeof buf, kCFStringEncodingUTF8);
            CFRelease(m);
        }
        fprintf(stderr, "keychain read failed (%d): %s\n", (int)st, buf);
        return 1;
    }

    CFDataRef d = (CFDataRef)out;
    fwrite(CFDataGetBytePtr(d), 1, (size_t)CFDataGetLength(d), stdout);
    fputc('\n', stdout);
    CFRelease(out);
    return 0;
}
