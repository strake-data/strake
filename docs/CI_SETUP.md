# CI Secrets Setup Guide

To enable full CI/CD functionality (including private submodule checkout), you must provide a credential that allows the CI environment to access the `strake-enterprise` repository.

## Comparison of Methods

| Method | Security | Link to Account | Recommendation |
| :--- | :--- | :--- | :--- |
| **Personal Access Token** | Lower | Tied to YOU | Use only for quick testing |
| **SSH Deploy Keys** | **High** | Repository-only | Best for small teams |
| **GitHub App** | **Highest** | Organization-wide | **Best for production** |

---

## 🛠️ Method A: SSH Deploy Keys (Recommended)
This is the most secure method for individual repository access.

1. **Generate Key Pair**:
   ```bash
   ssh-keygen -t ed25519 -f strake_deploy_key -N ""
   ```
2. **Add Public Key**:
   - Go to `strake-enterprise` **Settings > Deploy keys**.
   - Click "Add deploy key", paste the content of `strake_deploy_key.pub`.
3. **Add Private Key to Main Repo**:
   - Go to `strake` **Settings > Secrets and variables > Actions**.
   - Add a new secret named `SUBMODULE_TOKEN_SSH` and paste the content of `strake_deploy_key`.
   - Update your workflow to use an SSH agent step.

---

## 🛡️ Method B: GitHub App (Corporate Standard)
This is the most robust and secure way to handle automated access. It is not tied to any personal user account.

### 1. Create the GitHub App
1. Go to your **Organization Settings > Developer settings > GitHub Apps**.
2. Click **"New GitHub App"**.
3. **App Name**: `Strake CI Checkout`.
4. **Homepage URL**: `https://github.com/strake-data/strake`.
5. **Webhooks**: Uncheck "Active" (not needed).
6. **Permissions**: 
   - Under **Repository permissions**, find **Contents** and set it to **Read-only**.
7. **Create GitHub App**.

### 2. Generate Credentials
1. **App ID**: Note the "App ID" displayed at the top of the app settings.
2. **Private Key**: Scroll down to the bottom and click **"Generate a private key"**. This will download a `.pem` file.
3. **Install App**: In the left sidebar of the app settings, click **"Install App"** and install it on your organization (or just the `strake` and `strake-enterprise` repositories).

### 3. Add Secrets to `strake` Repo
Add these to **Settings > Secrets and variables > Actions**:
- `GH_APP_ID`: The App ID you noted.
- `GH_APP_PRIVATE_KEY`: The entire content of the `.pem` file you downloaded.

### 4. Update the Workflow
You will need to add a step to the workflow to generate a temporary token from the App ID/Secret (already implemented in `ci.yml` and `release.yml`).

```yaml
      - name: Generate Token
        id: generate-token
        uses: actions/create-github-app-token@v1
        with:
           app-id: ${{ secrets.GH_APP_ID }}
           private-key: ${{ secrets.GH_APP_PRIVATE_KEY }}
           owner: ${{ github.repository_owner }}

      - uses: actions/checkout@v4
        with:
          submodules: 'recursive'
          token: ${{ steps.generate-token.outputs.token }}
```

---

## ⚠️ Method C: Personal Access Token (PAT)
The easiest but least secure method. Use a **Fine-grained token** if possible.

1. **Generate PAT**: [GitHub Settings > Developer Settings](https://github.com/settings/tokens?type=beta).
2. **Permissions**: Grant "Read-only" access to the `strake-enterprise` repository.
3. **Add Secret**: Add it as `SUBMODULE_TOKEN` in `strake`.
