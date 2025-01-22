# Dominion Energy Home Assistant Integration 🔌⚡

A custom Home Assistant integration that grabs your Dominion Energy usage data and turns it into nice looking graphs and useful sensors.

## ⚠️ Alpha Software Warning ⚠️

This integration is a fun personal project that I built to scratch my own itch. There is no Dominion Energy API, that I have been able to find, so this integration uses a scraper to download the usage data. It WILL break if there are any significant changes to their website.

### What does that mean?

- It works! (on my machine™)
- There might be bugs (definitely are bugs)
- Updates happen when I have time and feel like working on my Home Assistant server
- Breaking changes are not just possible, they're probable
- Support is best-effort

## Features

- Logs into Dominion Energy website
- Downloads your usage data (hopefully)
- Creates sensors for:
    - Daily energy usage
    - Weekly trends
    - Current billing period usage and costs (usually)
    - Adds energy data to the Energy Dashboard

## Installation
<details>
<summary>Manual Installation</summary>

1. Copy the `custom_components/dominion_energy` directory into your Home Assistant's `custom_components` folder
2. Restart Home Assistant
3. Hope for the best! 🎲

> **Note**: When downloading files manually, use the `Raw` button on GitHub. Your browser's "Save As" might add unexpected formatting that will make Home Assistant very confused.
</details>

### Requirements

- Home Assistant 2025.1.0 or newer (it might work on older versions)
- A Dominion Energy account with online access
- A sense of adventure and patience for alpha software
- Chrome browser installed, the integration will attempt to install a headless version.

After installation, configure the integration through the UI:
1. Go to Settings -> Devices & Services
2. Click the "+ Add Integration" button
3. Search for "Dominion Energy"
4. Enter your Dominion Energy credentials
5. Cross your fingers and watch the magic happen! ✨

### Updating

- **HACS users**: HACS will notify you of updates. Whether you should install them is another question entirely! 😅
- **Manual installation**: You'll need to repeat the manual installation steps. Consider this your opportunity to check the latest code changes before updating!

Remember: This is alpha software! Always check the release notes before updating, and maybe keep a backup of your working configuration. Just in case. 😉

## Contribution

Found a bug? Want to add a feature? Feel free to submit a PR! Just remember:
- This is a Kotlin programmer's Python code
- I use Polars instead of Pandas
- Type hints are generally not optional
- Single letter / inscrutable variable names are generally discouraged. 

## License
Apache License 2.0 - See [LICENSE](LICENSE) for more information.

This software is provided on an "as is" basis, without warranties or conditions of any kind, either express or implied including, without limitation, any warranties or conditions of title, non-infringement, merchantability or fitness for a particular purpose.

## Disclaimer

This project is not affiliated with Dominion Energy. It's just a fun project by someone who really likes automating things.

Integration created originally from the [integration_bluprint](https://github.com/ludeeus/integration_blueprint) template