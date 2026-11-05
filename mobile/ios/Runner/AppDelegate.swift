import Flutter
import UIKit
import UserNotifications
import Security
import StoreKit

@main
@objc class AppDelegate: FlutterAppDelegate {
  private var proStore: AnyObject?
  private var pushChannel: FlutterMethodChannel?
  private var currentToken: String?
  private let preference = "PhillyScanAnnouncementsEnabled"

  override func application(_ application: UIApplication,
    didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
    GeneratedPluginRegistrant.register(with: self)
    if let controller = window?.rootViewController as? FlutterViewController {
      if #available(iOS 15.0, *) {
        proStore = ProStore(messenger: controller.binaryMessenger)
      }
      let channel = FlutterMethodChannel(name: "philly_scan/notifications", binaryMessenger: controller.binaryMessenger)
      pushChannel = channel
      channel.setMethodCallHandler { [weak self] call, result in
        guard let self = self else { return }
        switch call.method {
        case "status":
          UNUserNotificationCenter.current().getNotificationSettings { settings in
            DispatchQueue.main.async { result(self.status(settings)) }
          }
        case "enable":
          UNUserNotificationCenter.current().requestAuthorization(options: [.alert, .sound, .badge]) { granted, error in
            DispatchQueue.main.async {
              UserDefaults.standard.set(granted, forKey: self.preference)
              if granted { UIApplication.shared.registerForRemoteNotifications() }
              result(granted)
            }
          }
        case "disable":
          UserDefaults.standard.set(false, forKey: self.preference)
          UIApplication.shared.unregisterForRemoteNotifications()
          result(nil)
        case "refresh":
          if UserDefaults.standard.bool(forKey: self.preference) { UIApplication.shared.registerForRemoteNotifications() }
          result(nil)
        case "registration":
          guard let identity = self.identity() else {
            result(FlutterError(code: "keychain", message: "Could not save notification registration", details: nil)); return
          }
          var registration = identity
          registration["token"] = self.currentToken ?? ""
          registration["environment"] = (Bundle.main.object(forInfoDictionaryKey: "PhillyAPNSEnvironment") as? String) == "production" ? "production" : "sandbox"
          result(registration)
        case "openSettings":
          if let url = URL(string: UIApplication.openSettingsURLString) { UIApplication.shared.open(url) }
          result(nil)
        default: result(FlutterMethodNotImplemented)
        }
      }
    }
    UNUserNotificationCenter.current().delegate = self
    UNUserNotificationCenter.current().getNotificationSettings { settings in
      if settings.authorizationStatus == .authorized && UserDefaults.standard.bool(forKey: self.preference) {
        DispatchQueue.main.async { UIApplication.shared.registerForRemoteNotifications() }
      }
    }
    return super.application(application, didFinishLaunchingWithOptions: launchOptions)
  }
  private func status(_ settings: UNNotificationSettings) -> [String: Any] {
    return ["authorized": settings.authorizationStatus == .authorized,
      "denied": settings.authorizationStatus == .denied,
      "enabled": UserDefaults.standard.bool(forKey: preference)]
  }
  private func identity() -> [String: String]? {
    let query: [String: Any] = [kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: "com.dorian.phillyscan.push", kSecAttrAccount as String: "installation"]
    var lookup = query
    lookup[kSecReturnData as String] = true
    var item: CFTypeRef?
    let status = SecItemCopyMatching(lookup as CFDictionary, &item)
    if status == errSecSuccess, let data = item as? Data,
      let value = try? JSONSerialization.jsonObject(with: data) as? [String: String] { return value }
    guard status == errSecItemNotFound else { return nil }
    var bytes = [UInt8](repeating: 0, count: 32)
    guard SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes) == errSecSuccess else { return nil }
    let value = ["id": UUID().uuidString.lowercased(), "secret": bytes.map { String(format: "%02x", $0) }.joined()]
    guard let data = try? JSONSerialization.data(withJSONObject: value) else { return nil }
    var insert = query
    insert[kSecValueData as String] = data
    insert[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
    return SecItemAdd(insert as CFDictionary, nil) == errSecSuccess ? value : nil
  }
  override func application(_ application: UIApplication, didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data) {
    currentToken = deviceToken.map { String(format: "%02x", $0) }.joined()
    pushChannel?.invokeMethod("tokenReady", arguments: nil)
    super.application(application, didRegisterForRemoteNotificationsWithDeviceToken: deviceToken)
  }
  override func application(_ application: UIApplication, didFailToRegisterForRemoteNotificationsWithError error: Error) {
    pushChannel?.invokeMethod("registrationFailed", arguments: nil)
    super.application(application, didFailToRegisterForRemoteNotificationsWithError: error)
  }
  override func userNotificationCenter(_ center: UNUserNotificationCenter,
    willPresent notification: UNNotification, withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void) {
    completionHandler(UserDefaults.standard.bool(forKey: preference) ? [.alert, .sound] : [])
  }
  override func userNotificationCenter(_ center: UNUserNotificationCenter,
    didReceive response: UNNotificationResponse, withCompletionHandler completionHandler: @escaping () -> Void) {
    if response.notification.request.content.userInfo["kind"] as? String == "app_update",
      let url = URL(string: "https://apps.apple.com/us/app/philly-scan/id6758272125") {
      UIApplication.shared.open(url)
    }
    completionHandler()
  }
}


/// StoreKit validates Apple signatures on device. No app-written entitlement flag.
@available(iOS 15.0, *)
@MainActor
final class ProStore {
  private let channel: FlutterMethodChannel
  private let productID = "com.dorian.phillyscan.pro.monthly"
  private var updates: Task<Void, Never>?
  private var purchasing = false

  init(messenger: FlutterBinaryMessenger) {
    channel = FlutterMethodChannel(name: "philly_scan/pro", binaryMessenger: messenger)
    channel.setMethodCallHandler { [weak self] call, reply in
      Task { @MainActor in
        guard let self = self else { return }
        do {
          switch call.method {
          case "status": reply(await self.entitlement())
          case "product":
            let product = try await self.product()
            reply(["price": product.displayPrice, "id": product.id])
          case "redeem":
            if #available(iOS 16.0, *), let scene = UIApplication.shared.connectedScenes
              .compactMap({ $0 as? UIWindowScene }).first(where: { $0.activationState == .foregroundActive }) {
              try await AppStore.presentOfferCodeRedeemSheet(in: scene)
            } else {
              SKPaymentQueue.default().presentCodeRedemptionSheet()
            }
            // Closing the sheet is not proof of redemption; only verified entitlements unlock.
            reply(await self.entitlement())
          case "restore":
            try await AppStore.sync()
            reply(await self.entitlement())
          case "purchase":
            guard !self.purchasing else {
              reply(FlutterError(code: "busy", message: "A purchase is already in progress.", details: nil)); return
            }
            self.purchasing = true
            defer { self.purchasing = false }
            let product = try await self.product()
            switch try await product.purchase() {
            case .success(let verification):
              guard case .verified(let transaction) = verification,
                transaction.productID == self.productID else {
                reply(FlutterError(code: "verification", message: "Apple could not verify this purchase. Try Restore Purchases.", details: nil)); return
              }
              let access = await self.entitlement()
              self.channel.invokeMethod("changed", arguments: access)
              await transaction.finish()
              reply(access)
            case .pending: reply(["outcome": "pending"])
            case .userCancelled: reply(["outcome": "cancelled"])
            @unknown default: reply(["outcome": "unknown"])
            }
          default: reply(FlutterMethodNotImplemented)
          }
        } catch {
          reply(FlutterError(code: "store", message: "The App Store is unavailable or Pro is not configured. Please try again.", details: nil))
        }
      }
    }
    updates = Task { [weak self] in
      for await verification in Transaction.updates {
        guard let self = self else { return }
        guard case .verified(let transaction) = verification,
          transaction.productID == self.productID else { continue }
        self.channel.invokeMethod("changed", arguments: await self.entitlement())
        await transaction.finish()
      }
    }
  }
  deinit { updates?.cancel() }
  private func product() async throws -> Product {
    guard let product = try await Product.products(for: [productID]).first,
      product.type == .autoRenewable,
      let period = product.subscription?.subscriptionPeriod,
      period.unit == .month, period.value == 1 else {
      throw NSError(domain: "PhillyScan.Pro", code: 1)
    }
    return product
  }
  private func entitlement() async -> [String: Any] {
    var expiry: Date?
    for await verification in Transaction.currentEntitlements {
      guard case .verified(let transaction) = verification,
        transaction.productID == productID,
        transaction.productType == .autoRenewable,
        transaction.revocationDate == nil, !transaction.isUpgraded,
        let end = transaction.expirationDate, end > Date() else { continue }
      if expiry == nil || end > expiry! { expiry = end }
    }
    guard let expiry = expiry else { return ["active": false] }
    return ["active": true, "expiresAt": expiry.timeIntervalSince1970 * 1000]
  }
}
