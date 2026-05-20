"""
Setuptools hooks for platform-tagged wheels.
"""

from setuptools import find_packages, setup


SETUP_KWARGS = {
    'name': 'moq-rust-video',
    'version': '0.1.0',
    'description': 'Python control API for Rust MoQ real-time AVC3/H.264 video',
    'python_requires': '>=3.10',
    'packages': find_packages(
        include=['moq_official_relay*', 'moq_rust_client*', 'moq_rust_video*']
    ),
    'include_package_data': True,
    'package_data': {
        'moq_rust_video': [
            'py.typed',
            'bin/win_amd64/*',
            'bin/manylinux_x86_64/*',
            'bin/macosx_arm64/*',
            'bin/macosx_x86_64/*',
        ],
    },
}


try:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
except ModuleNotFoundError:
    setup(**SETUP_KWARGS)
else:

    class bdist_wheel(_bdist_wheel):
        """Mark wheels as platform-specific because they may contain moq-cli binaries."""

        def finalize_options(self):
            super().finalize_options()
            self.root_is_pure = False

    setup(**SETUP_KWARGS, cmdclass={'bdist_wheel': bdist_wheel})
