# Alto Integration Guide

This document outlines the Alto (Zoopla) integration for Property Eye, covering authentication, environment differences, and the operational workflow for activation.

## Authentication

Property Eye uses **OAuth2 Client Credentials** flow to authenticate with Alto APIs.

- **Client ID & Secret**: Stored in environment variables/Secrets Manager.
- **Token Endpoint**: `https://oauth.zoopla.co.uk/oauth/token`
- **Scope**: Access is scoped to the integration level.

## Environments

### Sandbox

- **API URL**: `https://mobile-api.zoopla.co.uk/sandbox/v1`
- **Agency Context**: The sandbox tenant is pre-wired to the integration.
- **AgencyRef**: **NOT** used or sent. Requests do not require an agency identifier header.

### Production

- **API URL**: `https://mobile-api.zoopla.co.uk/v1`
- **Agency Context**: A single integration serves multiple agencies.
- **AgencyRef**: **REQUIRED**. Every API call must identify the target agency using the `AgencyRef` header (or configured parameter).
- **Activation**: We receive a unique `AgencyRef` for each agency via email upon their Marketplace activation.

## Operational Workflow for Admins

When a real estate agency activates Property Eye in the Alto Marketplace:

1.  **Receive Email**: The Property Eye team receives an "Activation Request" email from Zoopla/Alto.
2.  **Extract Ref**: Copy the `AgencyRef` string from the email body.
3.  **Configure Agency**:
    - Log in to Property Eye Admin Dashboard.
    - Navigate to **Alto Integration**.
    - Find the activation agency in the list.
    - Click **Edit**.
    - Toggle **Enable Alto in Production**.
    - Paste the `AgencyRef` into the field.
    - Click **Save**.
4.  **Verification**: The system will now include this `AgencyRef` in all outbound production API calls for this agency.

## Troubleshooting

- **Missing AgencyRef Error**: If logs show 400/403 errors related to missing agency context in Production, verify the `AgencyRef` is correctly entered in the Admin panel.
- **Sandbox Testing**: Do not attempt to use production `AgencyRef`s in Sandbox; they are not recognized.
