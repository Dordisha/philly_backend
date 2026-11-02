import 'package:flutter/material.dart';
import '../src/api_client.dart';
import 'premium_features.dart';

class ProOverviewPage extends StatelessWidget {
  final String opa;
  final String address;
  final ApiClient? client;
  const ProOverviewPage({
    super.key,
    required this.opa,
    required this.address,
    this.client,
  });
  @override
  Widget build(BuildContext context) => Scaffold(
    appBar: AppBar(title: const Text('Explore property')),
    body: ListView(
      padding: const EdgeInsets.all(20),
      children: [
        Text(
          address,
          style: const TextStyle(fontSize: 23, fontWeight: FontWeight.w700),
        ),
        Text('OPA $opa'),
        FeatureMenu(opa: opa, address: address, client: client),
      ],
    ),
  );
}
